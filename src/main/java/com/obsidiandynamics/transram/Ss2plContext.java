package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.mutex.StripedMutexes.*;

import java.util.*;

public final class Ss2plContext<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final long mutexTimeoutMs;

  private final Ss2plMap<K, V> map;

  private final Set<MutexRef<UpgradeableMutex>> readMutexes = new HashSet<>();

  private final Set<MutexRef<UpgradeableMutex>> writeMutexes = new HashSet<>();

  private final Map<K, Versioned<V>> localValues = new HashMap<>();

  private final Set<K> stagedWrites = new HashSet<>();

  private long version;

  private State state = State.OPEN;

  Ss2plContext(Ss2plMap<K, V> map, long mutexTimeoutMs) {
    this.map = map;
    this.mutexTimeoutMs = mutexTimeoutMs;
  }

  @Override
  public V read(K key) throws ConcurrentModeFailure {
    ensureOpen();
    final var existing = localValues.get(key);
    if (existing != null) {
      return existing.getValue();
    }

    final var mutex = map.getMutexes().forKey(key);
    // don't lock for reading if we already have a write lock
    if (! writeMutexes.contains(mutex)) {
      final var addedMutex = readMutexes.add(mutex);
      if (addedMutex) {
        try {
          if (!mutex.mutex().tryReadAcquire(mutexTimeoutMs)) {
            readMutexes.remove(mutex);
            rollback();
            throw new ConcurrentModeFailure("Timed out while acquiring read mutex for key " + key, null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new ConcurrentModeFailure("Interrupted while acquiring read mutex for key " + key, e);
        }
      }
    }

    final Versioned<V> versioned = map.getStore().get(key);
    if (versioned != null) {
      final var cloned = versioned.deepClone();
      localValues.put(key, cloned);
      return cloned.getValue();
    } else {
      localValues.put(key, Versioned.unset());
      return null;
    }
  }

  @Override
  public void write(K key, V value) throws ConcurrentModeFailure {
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
            throw new ConcurrentModeFailure("Timed out while upgrading mutex for key " + key, null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new ConcurrentModeFailure("Interrupted while upgrading mutex for key " + key, e);
        }
      } else {
        try {
          if (!mutex.mutex().tryWriteAcquire(mutexTimeoutMs)) {
            writeMutexes.remove(mutex);
            rollback();
            throw new ConcurrentModeFailure("Timed out while acquiring write mutex for key " + key, null);
          }
        } catch (InterruptedException e) {
          rollback();
          throw new ConcurrentModeFailure("Interrupted while acquiring write mutex for key " + key, e);
        }
      }
    }

    stagedWrites.add(key);
    localValues.put(key, new Versioned<>(-1, value));
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
