package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.mutex.StripedMutexes.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.atomic.*;

public final class SrmlContext<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final SrmlMap<K, V> map;

  private final Set<Key> reads = new HashSet<>();

  private final Set<Key> writes = new HashSet<>();

  private final Map<Key, RawVersioned> localValues = new HashMap<>();

  private enum ItemState {
    INSERTED, EXISTING, DELETED
  }

  private final Map<Key, ItemState> itemStates = new HashMap<>();

  private final long readVersion;

  private long writeVersion = -1;

  private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);

  SrmlContext(SrmlMap<K, V> map) {
    this.map = map;

    readVersion = map.safeReadVersion().get();
  }

  @Override
  public V read(K key) throws BrokenSnapshotFailure {
    return Unsafe.cast(__read(WrapperKey.wrap(key)));
  }

  private Object __read(Key key) throws BrokenSnapshotFailure {
    ensureOpen();
    final var existing = localValues.get(key);
    if (existing != null) {
      return Unsafe.cast(existing.getValue());
    }

    // don't enrol as a read if it already appears as a write
    if (! writes.contains(key)) {
      reads.add(key);
    }

    final var storedValues = map.getStore().get(key);
    if (storedValues == null) {
      final var nullValue = new RawVersioned(readVersion, null);
      localValues.put(key, nullValue);
      itemStates.put(key, ItemState.DELETED);
      return null;
    } else {
      for (var storedValue : storedValues) {
        if (storedValue.getVersion() <= readVersion) {
          final var clonedValue = storedValue.deepClone();
          itemStates.put(key, clonedValue.hasValue() ? ItemState.EXISTING : ItemState.DELETED);
          localValues.put(key, clonedValue);
          return Unsafe.cast(clonedValue.getValue());
        }
      }

      throw new BrokenSnapshotFailure("Unable to restore value for key " + key + " at version " + readVersion + ", current at " + storedValues.getFirst().getVersion());
    }
  }

  @Override
  public void insert(K key, V value) throws BrokenSnapshotFailure {
    Assert.that(key != null, () -> "Cannot insert null key");
    Assert.that(value != null, () -> "Cannot insert null value");
    final var wrappedKey = WrapperKey.wrap(key);
    write(wrappedKey, value);
    final var lastItemState = itemStates.put(wrappedKey, ItemState.INSERTED);
    if (lastItemState == ItemState.EXISTING) {
      throw new IllegalStateException("Cannot insert over an existing item for key " + key);
    }
    alterSize(1);
  }

  @Override
  public void update(K key, V value) {
    Assert.that(key != null, () -> "Cannot update null key");
    Assert.that(value != null, () -> "Cannot update null value");
    final var wrappedKey = WrapperKey.wrap(key);
    write(wrappedKey, value);
    final var lastItemState = itemStates.put(wrappedKey, ItemState.EXISTING);
    if (lastItemState == ItemState.DELETED) {
      throw new IllegalStateException("Cannot update a deleted item for key " + key);
    }
  }

  @Override
  public void delete(K key) throws BrokenSnapshotFailure {
    Assert.that(key != null, () -> "Cannot delete null key");
    final var wrappedKey = WrapperKey.wrap(key);
    write(wrappedKey, null);
    final var lastItemState = itemStates.put(wrappedKey, ItemState.DELETED);
    if (lastItemState == ItemState.DELETED) {
      throw new IllegalStateException("Cannot deleted a previously deleted item for key " + key);
    }
    alterSize(-1);
  }

  private void write(Key key, DeepCloneable<?> value) {
    ensureOpen();
    localValues.put(key, new RawVersioned(-1, value));
    writes.add(key);
  }

  private void alterSize(int sizeChange) throws BrokenSnapshotFailure {
    final var size = (Size) __read(InternalKey.SIZE);
    Assert.that(size != null, () -> "No size object");
    size.set(size.get() + sizeChange);
    write(InternalKey.SIZE, size);
  }

  @Override
  public int size() throws BrokenSnapshotFailure {
    final var size = (Size) __read(InternalKey.SIZE);
    Assert.that(size != null, () -> "No size object");
    return size.get();
  }

  @Override
  public void rollback() {
    ensureOpen();
    state.set(State.ROLLED_BACK);
  }

  private void ensureOpen() {
    if (state.get() != State.OPEN) {
      throw new IllegalStateException("Transaction is not open");
    }
  }

  private static class LockModeAndState {
    LockMode mode;
    boolean locked;

    LockModeAndState(LockMode mode) {
      this.mode = mode;
    }
  }

  private enum LockMode {
    READ, WRITE
  }

  @Override
  public void close() throws MutexAcquisitionFailure, AntidependencyFailure, LifecycleFailure {
    if (state.get() != State.OPEN) {
      return;
    }

    final var combinedMutexes = new TreeMap<MutexRef<Mutex>, LockModeAndState>();
    for (var read : reads) {
      final var mutex = getMutex(read);
      if (writes.contains(read)) {
        combinedMutexes.put(mutex, new LockModeAndState(LockMode.WRITE));
      } else if (! combinedMutexes.containsKey(mutex)) {
        combinedMutexes.put(mutex, new LockModeAndState(LockMode.READ));
      }
    }

    for (var write : writes) {
      final var mutex = getMutex(write);
      combinedMutexes.put(mutex, new LockModeAndState(LockMode.WRITE));
    }

    for (var mutexEntry : combinedMutexes.entrySet()) {
      final var mutex = mutexEntry.getKey();
      final var lockModeAndState = mutexEntry.getValue();
      switch (lockModeAndState.mode) {
        case READ -> {
          try {
            mutex.mutex().tryReadAcquire(Long.MAX_VALUE);
          } catch (InterruptedException e) {
            rollbackFromCommitAttempt(combinedMutexes);
            throw new MutexAcquisitionFailure("Interrupted while acquiring read lock", e);
          }
        }
        case WRITE -> {
          try {
            mutex.mutex().tryWriteAcquire(Long.MAX_VALUE);
          } catch (InterruptedException e) {
            rollbackFromCommitAttempt(combinedMutexes);
            throw new MutexAcquisitionFailure("Interrupted while acquiring write lock", e);
          }
        }
      }
      lockModeAndState.locked = true;
    }

    for (var read : reads) {
      final var storedValues = map.getStore().get(read);
      final long storedValueVersion;
      if (storedValues == null) {
        storedValueVersion = readVersion;
      } else {
        storedValueVersion = storedValues.getFirst().getVersion();
      }

      if (storedValueVersion > readVersion) {
        rollbackFromCommitAttempt(combinedMutexes);
        throw new AntidependencyFailure("Read dependency breached for key " + read + "; expected version " + readVersion + ", saw " + storedValueVersion);
      }
    }

    for (var stateEntry : itemStates.entrySet()) {
      final var key = stateEntry.getKey();
      if (writes.contains(key)) {
        final var existingValues = map.getStore().get(key);
        switch (stateEntry.getValue()) {
          case INSERTED -> {
            if (existingValues != null && existingValues.getFirst().hasValue()) {
              rollbackFromCommitAttempt(combinedMutexes);
              throw new LifecycleFailure("Attempting to insert an existing item for key " + key);
            }
          }
          case EXISTING -> {
            if (existingValues == null || !existingValues.getFirst().hasValue()) {
              rollbackFromCommitAttempt(combinedMutexes);
              throw new LifecycleFailure("Attempting to update an non-existent item for key " + key);
            }
          }
          case DELETED -> {
            if (existingValues == null || !existingValues.getFirst().hasValue()) {
              rollbackFromCommitAttempt(combinedMutexes);
              throw new LifecycleFailure("Attempting to delete a non-existent item for key " + key);
            }
          }
        }
      }
    }

    synchronized (map.getContextLock()) {
      writeVersion = map.incrementAndGetVersion();
      map.getQueuedContexts().addLast(this);
    }

    for (var write : writes) {
      final var replacementValue = new RawVersioned(writeVersion, localValues.get(write).getValue());
       map.getStore().compute(write, (__, previousValues) -> {
         if (previousValues == null) {
           return SrmlMap.wrapInDeque(replacementValue);
         } else {
           previousValues.addFirst(replacementValue);
           return previousValues;
         }
      });
    }

    releaseMutexes(combinedMutexes);
    state.set(State.COMMITTED);
    drainQueuedContexts();
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private void drainQueuedContexts() {
    final var queuedContexts = map.getQueuedContexts();
    long highestVersionPurged = 0;
    final var queueDepth = map.getQueueDepth();
    while (true) {
      final var oldest = queuedContexts.peekFirst();
      if (oldest != null) {
        final var oldestState = oldest.getState();
        if (oldestState != State.OPEN) {
          if (queuedContexts.remove(oldest)) {
            if (oldestState == State.COMMITTED) {
              highestVersionPurged = oldest.writeVersion;
              for (var write : oldest.writes) {
                final var values = map.getStore().get(write);
                synchronized (values) {
                  while (values.size() > queueDepth) {
                    values.removeLast();
                  }
                }
              }
            }
          }
        } else {
          break;
        }
      } else {
        break;
      }
    }

    if (highestVersionPurged != 0) {
      while (true) {
        final var existingSafeReadVersion = map.safeReadVersion().get();
        if (highestVersionPurged > existingSafeReadVersion) {
          final var updated = map.safeReadVersion().compareAndSet(existingSafeReadVersion, highestVersionPurged);
          if (updated) {
            break;
          }
        } else {
          break;
        }
      }
    }
  }

  private void releaseMutexes(SortedMap<MutexRef<Mutex>, LockModeAndState> combinedMutexes) {
    for (var mutexEntry : combinedMutexes.entrySet()) {
      final var mutex = mutexEntry.getKey();
      final var lockModeAndState = mutexEntry.getValue();
      if (lockModeAndState.locked) {
        switch (lockModeAndState.mode) {
          case READ -> mutex.mutex().readRelease();
          case WRITE -> mutex.mutex().writeRelease();
        }
      }
    }
  }

  private void rollbackFromCommitAttempt(SortedMap<MutexRef<Mutex>, LockModeAndState> combinedMutexes) {
    releaseMutexes(combinedMutexes);
    state.set(State.ROLLED_BACK);
    drainQueuedContexts();
  }

  @Override
  public State getState() {
    return state.get();
  }

  @Override
  public long getVersion() {
    if (state.get() != State.OPEN) {
      throw new IllegalStateException("Transaction is not committed");
    }
    return writeVersion;
  }

  /**
   * Obtains a mutex reference for the given {@code key} such that, for mutexes of width <i>N</i>,
   * regular keys map to stripes 0..(<i>N</i> – 2), while internal keys map to stripe
   * <i>N</i> – 1.
   *
   * @param key The key.
   * @return The {@link MutexRef}.
   */
  private MutexRef<Mutex> getMutex(Key key) {
    final var mutexes = map.getMutexes();
    final var stripes = mutexes.stripes();
    if (key instanceof InternalKey) {
      return mutexes.forStripe(stripes - 1);
    } else {
      return mutexes.forStripe(StripedMutexes.hash(key, stripes - 1));
    }
  }
}
