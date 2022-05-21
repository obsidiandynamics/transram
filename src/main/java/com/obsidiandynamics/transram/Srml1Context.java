package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.mutex.StripedMutexes.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.*;

public final class Srml1Context<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final Srml1Map<K, V> map;

  private final Set<K> reads = new HashSet<>();

  private final Set<K> writes = new HashSet<>();

  private final Map<K, Versioned<V>> localValues = new HashMap<>();

  private final long readVersion;

  private final Set<Srml1Context<K, V>> peerContexts = new CopyOnWriteArraySet<>();

  private final Map<K, Versioned<V>> backupValues = new ConcurrentHashMap<>();

  private long writeVersion = -1;

  private State state = State.OPEN;

  Srml1Context(Srml1Map<K, V> map) {
    this.map = map;

    synchronized (map.getContextLock()) {
      final var oldestQueuedContext = map.getQueuedContexts().peekFirst();
      if (oldestQueuedContext != null) {
        readVersion = oldestQueuedContext.writeVersion - 1;
        peerContexts.addAll(map.getQueuedContexts());
      } else {
        readVersion = map.getVersion();
      }
      map.getOpenContexts().add(this);
    }
  }

  private Versioned<V> findAmongPeers(K key) {
    for (var peerContext : peerContexts) {
      final var backupValue = peerContext.backupValues.get(key);
//      System.out.format("  %s, backupValue %s\n", Thread.currentThread().getName(), backupValue);
      if (backupValue != null && backupValue.getVersion() <= readVersion) {
        return backupValue;
      }
    }

    return null;
  }

  @Override
  public V read(K key) {
    ensureOpen();
    final var existing = localValues.get(key);
    if (existing != null) {
      return existing.getValue();
    }

    // don't enrol as a read if it already appears as a write
    if (! writes.contains(key)) {
      reads.add(key);
    }

    final var storedValue = map.getStore().get(key);
    if (storedValue == null) {
      final var backupValue = findAmongPeers(key);
      final var clonedValue = backupValue == null ? new Versioned<V>(readVersion, null) : backupValue.deepClone();
      localValues.put(key, clonedValue);
      return clonedValue.getValue();
    } else if (storedValue.getVersion() <= readVersion) {
      final var clonedValue = storedValue.deepClone();
      localValues.put(key, clonedValue);
      return clonedValue.getValue();
    } else {
      final var backupValue = findAmongPeers(key);
      Assert.that(backupValue != null, () -> String.format("Unable to restore value for key %s at version %d (current at %d)", key, readVersion, storedValue.getVersion()));
      final var clonedValue = backupValue.deepClone();
      localValues.put(key, clonedValue);
      return clonedValue.getValue();
    }
  }

  @Override
  public void write(K key, V value) {
    ensureOpen();
    localValues.put(key, new Versioned<>(-1, value));
    writes.add(key);
  }

  @Override
  public void rollback() {
    ensureOpen();
    synchronized (map.getContextLock()) {
      map.getOpenContexts().remove(this);
      state = State.ROLLED_BACK;
    }
  }

  private void ensureOpen() {
    if (state != State.OPEN) {
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
  public void close() throws AntidependencyFailure, MutexAcquisitionFailure {
    if (state != State.OPEN) {
      return;
    }

    final var combinedMutexes = new TreeMap<MutexRef<Mutex>, LockModeAndState>();
    for (var read : reads) {
      final var mutex = map.getMutexes().forKey(read);
      if (writes.contains(read)) {
        combinedMutexes.put(mutex, new LockModeAndState(LockMode.WRITE));
      } else if (! combinedMutexes.containsKey(mutex)) {
        combinedMutexes.put(mutex, new LockModeAndState(LockMode.READ));
      }
    }

    for (var write : writes) {
      final var mutex = map.getMutexes().forKey(write);
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
      final var storedValue = map.getStore().get(read);
      final long storedValueVersion;
      if (storedValue != null) {
        storedValueVersion = storedValue.getVersion();
      } else {
        final var backupValue = findAmongPeers(read);
        storedValueVersion = backupValue != null ? backupValue.getVersion() : readVersion;
      }

      if (storedValueVersion > readVersion) {
        rollbackFromCommitAttempt(combinedMutexes);
        throw new AntidependencyFailure("Read dependency breached for key " + read + "; expected version " + readVersion + ", saw " + storedValueVersion);
      }
    }

    synchronized (map.getContextLock()) {
      writeVersion = map.incrementAndGetVersion();
      map.getQueuedContexts().addLast(this);
      for (var openContext : map.getOpenContexts()) {
        openContext.peerContexts.add(this);
      }
    }

    for (var write : writes) {
      final var replacementValue = new Versioned<>(writeVersion, localValues.get(write).getValue());
      map.getStore().compute(write, (__, previousValue) -> {
        if (previousValue != null) {
          //          System.out.format("  %s, backing up %s (writeVersion %d)\n", Thread.currentThread().getName(), previousValue, writeVersion);
          backupValues.put(write, previousValue);
        }
        return replacementValue;
      });
    }

    releaseMutexes(combinedMutexes);
    synchronized (map.getContextLock()) {
      state = State.COMMITTED;
      map.getOpenContexts().remove(this);
      drainQueuedContexts();
    }
    //    System.out.format("  %s, committed readVersion %d, writeVersion %d\n", Thread.currentThread().getName(), readVersion, writeVersion);
  }

  private void drainQueuedContexts() {
    final var queuedContexts = map.getQueuedContexts();
    while (true) {
      final var oldest = queuedContexts.peekFirst();
      if (oldest != null && oldest.state != State.OPEN) {
        queuedContexts.removeFirst();
      } else {
        break;
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
    synchronized (map.getContextLock()) {
      state = State.ROLLED_BACK;
      map.getOpenContexts().remove(this);
      drainQueuedContexts();
    }
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
    return writeVersion;
  }
}
