package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.mutex.StripedMutexes.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.function.*;

public final class Ss2plContext<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final long mutexTimeoutMs;

  private final Ss2plMap<K, V> map;

  private final Set<MutexRef<UpgradeableMutex>> readMutexes = new HashSet<>();

  private final Set<MutexRef<UpgradeableMutex>> writeMutexes = new HashSet<>();

  private enum ItemState {
    INSERTED, EXISTING, DELETED
  }

  private static final class Tracker {
    DeepCloneable<?> value;
    ItemState state;
    boolean written;

    Tracker(DeepCloneable<?> value, ItemState state, boolean written) {
      this.value = value;
      this.state = state;
      this.written = written;
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
            throw new MutexAcquisitionFailure("Timed out while acquiring read mutex for key " + key, null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new MutexAcquisitionFailure("Interrupted while acquiring read mutex for key " + key, e);
        }
      }
    }

    final var stored = map.getStore().get(key);
    if (stored != null) {
      final var cloned = DeepCloneable.clone(Unsafe.cast(stored.getValue()));
      local.put(key, new Tracker(cloned, ItemState.EXISTING, false));
      return cloned;
    } else {
      local.put(key, new Tracker(null, ItemState.DELETED, false));
      return null;
    }
  }

  @Override
  public Set<K> keys(Predicate<K> predicate) throws MutexAcquisitionFailure {
    ensureOpen();
    size();
    final var keys = new HashSet<K>();
    for (var entry : map.getStore().entrySet()) {
      final var key = entry.getKey();
      if (key instanceof KeyRef && predicate.test(Unsafe.cast(((KeyRef<?>) key).unwrap()))) {
        final var tracker = local.get(key);
        if (tracker != null) {
          if (tracker.value != null) {
            keys.add(Unsafe.cast(key));
          }
        } else {
          if (entry.getValue().hasValue()) {
            keys.add(Unsafe.cast(key));
          }
        }
      }
    }
    return keys;
  }

  @Override
  public void insert(K key, V value) throws MutexAcquisitionFailure {
    Assert.that(value != null, () -> "Cannot insert null value");
    write(Key.wrap(key), value, ItemState.INSERTED);
    alterSize(1);
  }

  @Override
  public void update(K key, V value) throws MutexAcquisitionFailure {
    Assert.that(value != null, () -> "Cannot update null value");
    write(Key.wrap(key), value, ItemState.EXISTING);
  }

  @Override
  public void delete(K key) throws MutexAcquisitionFailure {
    write(Key.wrap(key), null, ItemState.DELETED);
    alterSize(-1);
  }

  private void write(Key key, DeepCloneable<?> value, ItemState state) throws MutexAcquisitionFailure {
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
            throw new MutexAcquisitionFailure("Timed out while upgrading mutex for key " + key, null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new MutexAcquisitionFailure("Interrupted while upgrading mutex for key " + key, e);
        }
      } else {
        try {
          if (!mutex.mutex().tryWriteAcquire(mutexTimeoutMs)) {
            writeMutexes.remove(mutex);
            rollback();
            throw new MutexAcquisitionFailure("Timed out while acquiring write mutex for key " + key, null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new MutexAcquisitionFailure("Interrupted while acquiring write mutex for key " + key, e);
        }
      }
    }

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
        existing.state = state;
        existing.written = true;
        return existing;
      } else {
        return new Tracker(value, state, true);
      }
    });
  }

  private void alterSize(int sizeChange) throws MutexAcquisitionFailure {
    final var size = (Size) read(InternalKey.SIZE);
    Assert.that(size != null, () -> "No size object");
    size.set(size.get() + sizeChange);
    write(InternalKey.SIZE, size, ItemState.EXISTING);
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
      throw new IllegalStateException("Transaction is not open");
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
    if (state != State.OPEN) {
      return;
    }

    for (var entry : local.entrySet()) {
      final var tracker = entry.getValue();
      if (tracker.written) {
        final var key = entry.getKey();
        final var existingValue = map.getStore().get(key);
        switch (tracker.state) {
          case INSERTED -> {
            if (existingValue != null) {
              rollback();
              throw new LifecycleFailure("Attempting to insert an existing item for key " + key);
            }
          }
          case EXISTING -> {
            if (existingValue == null) {
              rollback();
              throw new LifecycleFailure("Attempting to update an non-existent item for key " + key);
            }
          }
          case DELETED -> {
            if (existingValue == null) {
              rollback();
              throw new LifecycleFailure("Attempting to delete a non-existent item for key " + key);
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
    if (state != State.OPEN) {
      throw new IllegalStateException("Transaction is not committed");
    }
    return version;
  }
}
