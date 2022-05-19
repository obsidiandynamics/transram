package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.mutex.StripedMutexes.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class Srml2Context<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final Srml2Map<K, V> map;

  private final Set<K> reads = new HashSet<>();

  private final Set<K> writes = new HashSet<>();

  private final Map<K, Versioned<V>> localValues = new HashMap<>();

  private final long readVersion;

  private final Set<Srml2Context<K, V>> peerContexts = new CopyOnWriteArraySet<>();

  private final Map<K, Versioned<V>> backupValues = new ConcurrentHashMap<>();

  private long writeVersion = -1;

  private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);

  Srml2Context(Srml2Map<K, V> map) {
    this.map = map;

    map.getOpenContexts().add(this);
    final boolean hadQueued;
    synchronized (map.getContextLock()) {
      final var oldestQueuedContext = map.getQueuedContexts().peekFirst();
      if (oldestQueuedContext != null) {
        hadQueued = true;
        readVersion = oldestQueuedContext.writeVersion - 1;
      } else {
        hadQueued = false;
        readVersion = map.getVersion();
      }
    }

    if (hadQueued) {
      peerContexts.addAll(map.getQueuedContexts());
    }
  }

  private Versioned<V> findAmongPeers(K key) {
    for (var peerContext : peerContexts) {
      final var backupValue = peerContext.backupValues.get(key);
      if (backupValue != null && backupValue.getVersion() <= readVersion) {
        return backupValue;
      }
    }

    return null;
  }

  @Override
  public V read(K key) throws BrokenSnapshotFailure {
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
      if (backupValue == null) {
        throw new BrokenSnapshotFailure("Unable to restore value for key " + key + " at version " + readVersion + ", current at " + storedValue.getVersion());
      }
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
    }
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
  public void close() throws MutexAcquisitionFailure, AntidependencyFailure {
    if (state.get() != State.OPEN) {
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
    }

    for (var openContext : map.getOpenContexts()) {
      openContext.peerContexts.add(this);
    }

    for (var write : writes) {
      final var replacementValue = new Versioned<>(writeVersion, localValues.get(write).getValue());
      map.getStore().compute(write, (__, previousValue) -> {
        if (previousValue != null) {
          backupValues.put(write, previousValue);
        }
        return replacementValue;
      });
    }

    releaseMutexes(combinedMutexes);
    map.getOpenContexts().remove(this);
    state.set(State.COMMITTED);
    drainQueuedContexts();
  }

  private void drainQueuedContexts() {
    final var queuedContexts = map.getQueuedContexts();
    while (true) {
      final var oldest = queuedContexts.peekFirst();
      if (oldest != null && oldest.state.get() != State.OPEN) {
        queuedContexts.remove(oldest);
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
    map.getOpenContexts().remove(this);
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
}
