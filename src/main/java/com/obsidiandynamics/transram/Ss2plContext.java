package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.lock.StripeLocks.*;

import java.util.*;

public final class Ss2plContext<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final long lockTimeoutMs;

  private final Ss2plMap<K, V> map;

  private final Set<LockRef> readLocks = new HashSet<>();

  private final Set<LockRef> writeLocks = new HashSet<>();

  private final Map<K, Versioned<V>> localValues = new HashMap<>();

  private final Set<K> stagedWrites = new HashSet<>();

  private long version;

  private State state = State.OPEN;

  Ss2plContext(Ss2plMap<K, V> map, long lockTimeoutMs) {
    this.map = map;
    this.lockTimeoutMs = lockTimeoutMs;
  }

  @Override
  public V read(K key) throws ConcurrentModeFailure {
    ensureOpen();
    final var existing = localValues.get(key);
    if (existing != null) {
      return existing.getValue();
    }

    final var lock = map.getLocks().forKey(key);
    // don't lock for reading if we already have a write lock
    if (! writeLocks.contains(lock)) {
      final var addedLock = readLocks.add(lock);
      if (addedLock) {
        try {
          if (!lock.lock().tryReadAcquire(lockTimeoutMs)) {
            readLocks.remove(lock);
            rollback();
            throw new ConcurrentModeFailure("Timed out while acquiring read lock for key " + key, null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new ConcurrentModeFailure("Interrupted while acquiring read lock for key " + key, e);
        }
      }
    }

    final Versioned<V> versioned = map.getStore().get(key);
    if (versioned != null) {
      final var cloned = versioned.getValue().deepClone();
      localValues.put(key, new Versioned<>(versioned.getVersion(), cloned));
      return cloned;
    } else {
      localValues.put(key, Versioned.unset());
      return null;
    }
  }

  @Override
  public void write(K key, V value) throws ConcurrentModeFailure {
    ensureOpen();
    final var lock = map.getLocks().forKey(key);
    final var addedLock = writeLocks.add(lock);
    if (addedLock) {
      final var readLockAcquired = readLocks.remove(lock);
      if (readLockAcquired) {
        try {
          if (!lock.lock().tryUpgrade(lockTimeoutMs)) {
            readLocks.add(lock);
            writeLocks.remove(lock);
            rollback();
            throw new ConcurrentModeFailure("Timed out while upgrading lock for key " + key, null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new ConcurrentModeFailure("Interrupted while upgrading lock for key " + key, e);
        }
      } else {
        try {
          if (!lock.lock().tryWriteAcquire(lockTimeoutMs)) {
            writeLocks.remove(lock);
            rollback();
            throw new ConcurrentModeFailure("Timed out while acquiring write lock for key " + key, null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new ConcurrentModeFailure("Interrupted while acquiring write lock for key " + key, e);
        }
      }
    }

    stagedWrites.add(key);
    localValues.put(key, new Versioned<>(-1, value));
  }

  @Override
  public void rollback() {
    ensureOpen();
    releaseLocks();
    state = State.ROLLED_BACK;
  }

  private void ensureOpen() {
    if (state != State.OPEN) {
      throw new IllegalStateException("Transaction is not open");
    }
  }

  private void releaseLocks() {
    for (var lock : readLocks) {
      lock.lock().readRelease();
    }
    for (var lock : writeLocks) {
      lock.lock().writeRelease();
    }
  }

  @Override
  public void close() {
    if (state != State.OPEN) {
      return;
    }

    version = map.version().incrementAndGet();
    for (var key : stagedWrites) {
      final var local = localValues.get(key);
      if (local.hasValue()) {
        map.getStore().put(key, new Versioned<>(version, local.getValue()));
      } else {
        map.getStore().remove(key);
      }
    }
    releaseLocks();
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
