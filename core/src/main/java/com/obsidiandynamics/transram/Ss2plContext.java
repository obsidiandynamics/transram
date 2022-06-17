package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.LifecycleFailure.*;
import com.obsidiandynamics.transram.mutex.StripedMutexes.*;
import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.function.*;

public final class Ss2plContext<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final long mutexTimeoutMs;

  private final Ss2plMap<K, V> map;

  private final Set<MutexRef<UpgradeableMutex>> readMutexes = new HashSet<>();

  private final Set<MutexRef<UpgradeableMutex>> writeMutexes = new HashSet<>();

  private enum StateChange {
    INSERTED, UNCHANGED, DELETED
  }

  private static final class Tracker {
    DeepCloneable<?> value;
    boolean written;
    StateChange change;

    Tracker(DeepCloneable<?> value, boolean written, StateChange change) {
      this.value = value;
      this.written = written;
      this.change = change;
    }
  }

  private final Map<Key, Tracker> local = new HashMap<>();

  private long version;

  private State state = State.OPEN;

  Ss2plContext(Ss2plMap<K, V> map, long mutexTimeoutMs) {
    this.map = map;
    this.mutexTimeoutMs = mutexTimeoutMs;
  }

  @Override
  public V read(K key) throws MutexAcquisitionFailure {
    return Unsafe.cast(read(Key.wrap(key)));
  }

  private DeepCloneable<?> read(Key key) throws MutexAcquisitionFailure {
    ensureOpen();
    final var existing = local.get(key);
    if (existing != null) {
      return Unsafe.cast(existing.value);
    }

    final var mutex = map.getMutexes().forKey(key);
    // don't lock for reading if we already have a write lock
    if (!writeMutexes.contains(mutex)) {
      final var addedMutex = readMutexes.add(mutex);
      if (addedMutex) {
        try {
          if (!mutex.mutex().tryReadAcquire(mutexTimeoutMs)) {
            readMutexes.remove(mutex);
            rollback();
            throw new MutexAcquisitionFailure("Timed out while acquiring read mutex", null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new MutexAcquisitionFailure("Interrupted while acquiring read mutex", e);
        }
      }
    }

    final var stored = map.getStore().get(key);
    if (stored != null) {
      final var cloned = DeepCloneable.clone(Unsafe.cast(stored.getValue()));
      local.put(key, new Tracker(cloned, false, StateChange.UNCHANGED));
      return cloned;
    } else {
      local.put(key, new Tracker(null, false, StateChange.UNCHANGED));
      return null;
    }
  }

  @Override
  public Set<K> keys(Predicate<K> predicate) throws MutexAcquisitionFailure {
    ensureOpen();
    // doing an initial size() check acquires a lock on the size object, preventing further key insertions
    size();

    final var keys = new HashSet<K>();

    // start by checking upstream keys
    for (var entry : map.getStore().entrySet()) {
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
            if (entry.getValue().hasValue()) {
              keys.add(unwrapped);
            }
          }
        }
      }
    }

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
  public void insert(K key, V value) throws MutexAcquisitionFailure {
    Assert.that(value != null, NullValueAssertionError::new, () -> "Cannot insert null value");
    write(Key.wrap(key), value, StateChange.INSERTED);
    alterSize(1);
  }

  @Override
  public void update(K key, V value) throws MutexAcquisitionFailure {
    Assert.that(value != null, NullValueAssertionError::new, () -> "Cannot update null value");
    write(Key.wrap(key), value, StateChange.UNCHANGED);
  }

  @Override
  public void delete(K key) throws MutexAcquisitionFailure {
    write(Key.wrap(key), null, StateChange.DELETED);
    alterSize(-1);
  }

  private void write(Key key, DeepCloneable<?> value, StateChange change) throws MutexAcquisitionFailure {
    ensureOpen();
    final var mutex = map.getMutexes().forKey(key);
    final var addedMutex = writeMutexes.add(mutex);
    if (addedMutex) {
      final var readMutexAcquired = readMutexes.remove(mutex);
      if (readMutexAcquired) {
        try {
          if (!mutex.mutex().tryUpgrade(mutexTimeoutMs)) {
            readMutexes.add(mutex);
            writeMutexes.remove(mutex);
            rollback();
            throw new MutexAcquisitionFailure("Timed out while upgrading mutex", null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new MutexAcquisitionFailure("Interrupted while upgrading mutex", e);
        }
      } else {
        try {
          if (!mutex.mutex().tryWriteAcquire(mutexTimeoutMs)) {
            writeMutexes.remove(mutex);
            rollback();
            throw new MutexAcquisitionFailure("Timed out while acquiring write mutex", null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new MutexAcquisitionFailure("Interrupted while acquiring write mutex", e);
        }
      }
    }

    local.compute(key, (__, existing) -> {
      if (existing != null) {
        switch (change) {
          case INSERTED -> {
            if (existing.value != null) {
              throw new IllegalLifecycleStateException(IllegalLifecycleStateException.Reason.INSERT_EXISTING, "Cannot insert an existing item for key " + key);
            }
            switch (existing.change) {
              case UNCHANGED -> existing.change = Ss2plContext.StateChange.INSERTED;
              case DELETED -> existing.change = Ss2plContext.StateChange.UNCHANGED;
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
              case INSERTED -> existing.change = Ss2plContext.StateChange.UNCHANGED;
              case UNCHANGED -> existing.change = Ss2plContext.StateChange.DELETED;
            }
          }
        }
        existing.value = value;
        existing.written = true;
        return existing;
      } else {
        return new Tracker(value, true, change);
      }
    });
  }

  private void alterSize(int sizeChange) throws MutexAcquisitionFailure {
    final var size = (Size) read(InternalKey.SIZE);
    Assert.that(size != null, () -> "No size object");
    final var newSize = size.get() + sizeChange;
    if (newSize < 0) {
      throw new IllegalLifecycleStateException(IllegalLifecycleStateException.Reason.NEGATIVE_SIZE, "Negative size after delete");
    }
    size.set(newSize);
    write(InternalKey.SIZE, size, StateChange.UNCHANGED);
  }

  @Override
  public int size() throws MutexAcquisitionFailure {
    final var size = (Size) read(InternalKey.SIZE);
    Assert.that(size != null, () -> "No size object");
    return size.get();
  }

  @Override
  public void rollback() {
    ensureOpen();
    releaseMutexes();
    state = State.ROLLED_BACK;
  }

  private void ensureOpen() {
    if (state != State.OPEN) {
      throw new TransactionNotOpenException();
    }
  }

  private void releaseMutexes() {
    for (var mutex : readMutexes) {
      mutex.mutex().readRelease();
    }
    for (var mutex : writeMutexes) {
      mutex.mutex().writeRelease();
    }
  }

  @Override
  public void commit() throws LifecycleFailure {
    ensureOpen();

    for (var entry : local.entrySet()) {
      final var tracker = entry.getValue();
      if (tracker.written) {
        final var key = entry.getKey();
        final var existingValue = map.getStore().get(key);
        switch (tracker.change) {
          case INSERTED -> {
            if (existingValue != null) {
              rollback();
              throw new LifecycleFailure(Reason.INSERT_EXISTING, "Attempting to insert an existing item for key " + key);
            }
          }
          case UNCHANGED -> {
            if (entry.getValue().value != null && existingValue == null) {
              rollback();
              throw new LifecycleFailure(Reason.UPDATE_NONEXISTENT, "Attempting to update a nonexistent item for key " + key);
            }

            if (entry.getValue().value == null && existingValue != null) {
              rollback();
              throw new LifecycleFailure(Reason.INSERT_DELETE_EXISTING, "Attempting to insert-delete an existing item for key " + key);
            }
          }
          case DELETED -> {
            if (existingValue == null) {
              rollback();
              throw new LifecycleFailure(Reason.DELETE_NONEXISTENT, "Attempting to delete a nonexistent item for key " + key);
            }
          }
        }
      }
    }

    version = map.version().incrementAndGet();

    for (var entry : local.entrySet()) {
      final var tracker = entry.getValue();
      if (tracker.written) {
        if (tracker.value != null) {
          map.getStore().put(entry.getKey(), new RawVersioned(version, tracker.value));
        } else {
          map.getStore().remove(entry.getKey());
        }
      }
    }
    releaseMutexes();
    state = State.COMMITTED;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public long getVersion() {
    if (state != State.COMMITTED) {
      throw new TransactionNotCommittedException();
    }
    return version;
  }
}
