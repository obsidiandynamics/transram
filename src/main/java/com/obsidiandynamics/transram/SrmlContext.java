package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.mutex.StripedMutexes.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.atomic.*;

public final class SrmlContext<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final SrmlMap<K, V> map;

  private final Map<Key, Tracker> local = new HashMap<>();

  private enum ItemState {
    INSERTED, EXISTING, DELETED
  }

  private static final class Tracker {
    DeepCloneable<?> value;

    final boolean read;

    boolean written;

    ItemState state;

    Tracker(DeepCloneable<?> value, boolean read, boolean written, ItemState state) {
      this.value = value;
      this.read = read;
      this.written = written;
      this.state = state;
    }
  }

  private final long readVersion;

  private long writeVersion = -1;

  private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);

  SrmlContext(SrmlMap<K, V> map) {
    this.map = map;

    readVersion = map.safeReadVersion().get();
  }

  @Override
  public V read(K key) throws BrokenSnapshotFailure {
    return Unsafe.cast(read(Key.wrap(key)));
  }

  private Object read(Key key) throws BrokenSnapshotFailure {
    ensureOpen();
    final var existing = local.get(key);
    if (existing != null) {
      // don't enrol as a read if it already appears as a write
      return Unsafe.cast(existing.value);
    }

    final var storedValues = map.getStore().get(key);
    if (storedValues == null) {
      local.put(key, new Tracker(null, true, false, ItemState.DELETED));
      return null;
    } else {
      for (var storedValue : storedValues) {
        if (storedValue.getVersion() <= readVersion) {
          final var clonedValue = DeepCloneable.clone(Unsafe.cast(storedValue.getValue()));
          final var itemState = clonedValue != null ? ItemState.EXISTING : ItemState.DELETED;
          local.put(key, new Tracker(clonedValue, true, false, itemState));
          return clonedValue;
        }
      }

      throw new BrokenSnapshotFailure("Unable to restore value for key " + key + " at version " + readVersion + ", current at " + storedValues.getFirst().getVersion());
    }
  }

  @Override
  public void insert(K key, V value) throws BrokenSnapshotFailure {
    Assert.that(value != null, () -> "Cannot insert null value");
    write(Key.wrap(key), value, ItemState.INSERTED);
    alterSize(1);
  }

  @Override
  public void update(K key, V value) {
    Assert.that(value != null, () -> "Cannot update null value");
    write(Key.wrap(key), value, ItemState.EXISTING);
  }

  @Override
  public void delete(K key) throws BrokenSnapshotFailure {
    write(Key.wrap(key), null, ItemState.DELETED);
    alterSize(-1);
  }

  private void write(Key key, DeepCloneable<?> value, ItemState state) {
    ensureOpen();
    local.compute(key, (__, existing) -> {
      if (existing != null) {
        switch (state) {
          case INSERTED -> {
            if (existing.state == ItemState.EXISTING) {
              throw new IllegalStateException("Cannot insert an existing item for key " + key);
            }
          }
          case EXISTING -> {
            if (existing.state == ItemState.DELETED) {
              throw new IllegalStateException("Cannot update a deleted item for key " + key);
            }
          }
          case DELETED -> {
            if (existing.state == ItemState.DELETED) {
              throw new IllegalStateException("Cannot delete a non-existent item for key " + key);
            }
          }
        }
        existing.value = value;
        existing.written = true;
        existing.state = state;
        return existing;
      } else {
        return new Tracker(value, false, true, state);
      }
    });
  }

  private void alterSize(int sizeChange) throws BrokenSnapshotFailure {
    final var size = (Size) read(InternalKey.SIZE);
    Assert.that(size != null, () -> "No size object");
    size.set(size.get() + sizeChange);
    write(InternalKey.SIZE, size, ItemState.EXISTING);
  }

  @Override
  public int size() throws BrokenSnapshotFailure {
    final var size = (Size) read(InternalKey.SIZE);
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
  public void commit() throws MutexAcquisitionFailure, AntidependencyFailure, LifecycleFailure {
    ensureOpen();

    final var combinedMutexes = new TreeMap<MutexRef<Mutex>, LockModeAndState>();
    for (var entry : local.entrySet()) {
      final var tracker = entry.getValue();
      final var mutex = getMutex(entry.getKey());
      if (tracker.written) {
        combinedMutexes.put(mutex, new LockModeAndState(LockMode.WRITE));
      } else {
        combinedMutexes.put(mutex, new LockModeAndState(LockMode.READ));
      }
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

    for (var entry : local.entrySet()) {
      final var key = entry.getKey();
      final var tracker = entry.getValue();
      if (tracker.read) {
        final var storedValues = map.getStore().get(key);
        final long storedValueVersion;
        if (storedValues == null) {
          storedValueVersion = readVersion;
        } else {
          storedValueVersion = storedValues.getFirst().getVersion();
        }

        if (storedValueVersion > readVersion) {
          rollbackFromCommitAttempt(combinedMutexes);
          throw new AntidependencyFailure("Read dependency breached for key " + key + "; expected version " + readVersion + ", saw " + storedValueVersion);
        }
      }

      if (tracker.written) {
        final var existingValues = map.getStore().get(key);
        switch (entry.getValue().state) {
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

    for (var entry : local.entrySet()) {
      final var tracker = entry.getValue();
      if (tracker.written) {
        final var replacementValue = new RawVersioned(writeVersion, tracker.value);
        map.getStore().compute(entry.getKey(), (__, previousValues) -> {
          if (previousValues == null) {
            return SrmlMap.wrapInDeque(replacementValue);
          } else {
            previousValues.addFirst(replacementValue);
            return previousValues;
          }
        });
      }
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
              for (var entry : oldest.local.entrySet()) {
                if (entry.getValue().written) {
                  final var values = map.getStore().get(entry.getKey());
                  synchronized (values) {
                    while (values.size() > queueDepth) {
                      values.removeLast();
                    }
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
