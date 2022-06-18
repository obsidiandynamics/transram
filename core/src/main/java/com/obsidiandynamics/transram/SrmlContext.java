package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.LifecycleFailure.*;
import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.mutex.StripedMutexes.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

public final class SrmlContext<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final SrmlMap<K, V> map;

  private final Map<Key, Tracker> local = new HashMap<>();

  private enum StateChange {
    INSERTED, UNCHANGED, DELETED
  }

  private static final class Tracker {
    DeepCloneable<?> value;

    final boolean read;

    boolean written;

    StateChange change;

    Tracker(DeepCloneable<?> value, boolean read, boolean written, StateChange change) {
      this.value = value;
      this.read = read;
      this.written = written;
      this.change = change;
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

  private DeepCloneable<?> read(Key key) throws BrokenSnapshotFailure {
    ensureOpen();
    final var existing = local.get(key);
    if (existing != null) {
      // don't enrol as a read if it already appears as a write
      return Unsafe.cast(existing.value);
    }

    final var storedValues = map.getStore().get(key);
    if (storedValues == null) {
      local.put(key, new Tracker(null, true, false, StateChange.UNCHANGED));
      return null;
    } else {
      for (var storedValue : storedValues) {
        if (storedValue.getVersion() <= readVersion) {
          final var clonedValue = DeepCloneable.clone(Unsafe.cast(storedValue.getValue()));
          local.put(key, new Tracker(clonedValue, true, false, StateChange.UNCHANGED));
          return clonedValue;
        }
      }

      throw new BrokenSnapshotFailure("Unable to restore value for key " + key + " at version " + readVersion + ", current at " + storedValues.getFirst().getVersion());
    }
  }

  @Override
  public Set<K> keys(Predicate<K> predicate) throws BrokenSnapshotFailure {
    ensureOpen();
    final var keys = new HashSet<K>();

    // start by checking upstream keys
    entryLoop: for (var entry : map.getStore().entrySet()) {
      final var key = entry.getKey();
      if (key instanceof KeyRef) {
        final var unwrapped = Unsafe.<K>cast(((KeyRef<?>) key).unwrap());
        if (predicate.test(unwrapped)) {
          final var tracker = local.get(key);
          if (tracker != null) {
            if (tracker.value != null) {
              keys.add(unwrapped);
            }
          } else {
            final var storedValues = entry.getValue();
            for (var storedValue : storedValues) {
              if (storedValue.getVersion() <= readVersion) {
                if (storedValue.hasValue()) {
                  keys.add(unwrapped);
                }
                continue entryLoop;
              }
            }
            throw new BrokenSnapshotFailure("Unable to restore value for key " + key + " at version " + readVersion + ", current at " + storedValues.getFirst().getVersion());
          }
        }
      }
    }

    // a size() check at any point creates a dependency upon the size object, trapping insertion antidependencies
    size();

    // include locally staged keys that weren't present upstream
    for (var entry : local.entrySet()) {
      final var key = entry.getKey();
      if (key instanceof KeyRef) {
        final var unwrapped = Unsafe.<K>cast(((KeyRef<?>) key).unwrap());
        if (entry.getValue().value != null && !keys.contains(unwrapped) && predicate.test(unwrapped)) {
          keys.add(unwrapped);
        }
      }
    }
    return keys;
  }

  @Override
  public void insert(K key, V value) throws BrokenSnapshotFailure {
    Assert.that(value != null, NullValueAssertionError::new, () -> "Cannot insert null value");
    write(Key.wrap(key), value, StateChange.INSERTED);
    alterSize(1);
  }

  @Override
  public void update(K key, V value) {
    Assert.that(value != null, NullValueAssertionError::new, () -> "Cannot update null value");
    write(Key.wrap(key), value, StateChange.UNCHANGED);
  }

  @Override
  public void delete(K key) throws BrokenSnapshotFailure {
    write(Key.wrap(key), null, StateChange.DELETED);
    alterSize(-1);
  }

  private void write(Key key, DeepCloneable<?> value, StateChange change) {
    ensureOpen();
    local.compute(key, (__, existing) -> {
      if (existing != null) {
        switch (change) {
          case INSERTED -> {
            if (existing.value != null) {
              throw new IllegalLifecycleStateException(IllegalLifecycleStateException.Reason.INSERT_EXISTING, "Cannot insert an existing item for key " + key);
            }
            switch (existing.change) {
              case UNCHANGED -> existing.change = StateChange.INSERTED;
              case DELETED -> existing.change = StateChange.UNCHANGED;
            }
          }
          case UNCHANGED -> {
            if (existing.value == null) {
              throw new IllegalLifecycleStateException(IllegalLifecycleStateException.Reason.UPDATE_NONEXISTENT, "Cannot update a nonexistent item for key " + key);
            }
          }
          case DELETED -> {
            if (existing.value == null) {
              throw new IllegalLifecycleStateException(IllegalLifecycleStateException.Reason.DELETE_NONEXISTENT, "Cannot delete a nonexistent item for key " + key);
            }
            switch (existing.change) {
              case INSERTED -> existing.change = StateChange.UNCHANGED;
              case UNCHANGED -> existing.change = StateChange.DELETED;
            }
          }
        }
        existing.value = value;
        existing.written = true;
        return existing;
      } else {
        return new Tracker(value, false, true, change);
      }
    });
  }

  private void alterSize(int sizeChange) throws BrokenSnapshotFailure {
    final var size = (Size) read(InternalKey.SIZE);
    Assert.isNotNull(size, Assert.withMessage("No size object"));
    final var newSize = size.get() + sizeChange;
    if (newSize < 0) {
      throw new IllegalLifecycleStateException(IllegalLifecycleStateException.Reason.NEGATIVE_SIZE, "Negative size after delete");
    }
    size.set(newSize);
    write(InternalKey.SIZE, size, StateChange.UNCHANGED);
  }

  @Override
  public int size() throws BrokenSnapshotFailure {
    final var size = (Size) read(InternalKey.SIZE);
    Assert.isNotNull(size, Assert.withMessage("No size object"));
    return size.get();
  }

  @Override
  public void rollback() {
    ensureOpen();
    state.set(State.ROLLED_BACK);
  }

  private void ensureOpen() {
    if (state.get() != State.OPEN) {
      throw new TransactionNotOpenException();
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
        switch (entry.getValue().change) {
          case INSERTED -> {
            if (existingValues != null && existingValues.getFirst().hasValue()) {
              rollbackFromCommitAttempt(combinedMutexes);
              throw new LifecycleFailure(Reason.INSERT_EXISTING, "Attempting to insert an existing item for key " + key);
            }
          }
          case UNCHANGED -> {
            final var existsUpstream = existingValues != null && existingValues.getFirst().hasValue();
            if (entry.getValue().value != null && !existsUpstream) {
              rollbackFromCommitAttempt(combinedMutexes);
              throw new LifecycleFailure(Reason.UPDATE_NONEXISTENT, "Attempting to update a nonexistent item for key " + key);
            }
            if (entry.getValue().value == null && existsUpstream) {
              rollbackFromCommitAttempt(combinedMutexes);
              throw new LifecycleFailure(Reason.INSERT_DELETE_EXISTING, "Attempting to insert-delete an existing item for key " + key);
            }
          }
          case DELETED -> {
            if (existingValues == null || !existingValues.getFirst().hasValue()) {
              rollbackFromCommitAttempt(combinedMutexes);
              throw new LifecycleFailure(Reason.DELETE_NONEXISTENT, "Attempting to delete a nonexistent item for key " + key);
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
        if (oldestState == State.COMMITTED) {
          if (queuedContexts.remove(oldest)) {
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
        } else {
          break;
        }
      } else {
        break;
      }
    }

    if (highestVersionPurged != 0) {
      Cas.compareAndSetConditionally(map.safeReadVersion(), highestVersionPurged, Cas.lowerThan(highestVersionPurged));
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
    if (state.get() != State.COMMITTED) {
      throw new TransactionNotCommittedException();
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
