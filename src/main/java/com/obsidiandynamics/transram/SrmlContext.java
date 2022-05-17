package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.lock.StripeLocks.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.*;

public final class SrmlContext<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final SrmlMap<K, V> map;

  private final Set<K> reads = new HashSet<>();

  private final Set<K> writes = new HashSet<>();

  private final Map<K, Versioned<V>> localValues = new HashMap<>();

  private final long readVersion;

  private final Set<SrmlContext<K, V>> peerContexts = new CopyOnWriteArraySet<>();

  private final Map<K, Versioned<V>> backupValues = new ConcurrentHashMap<>();

  private long writeVersion = -1;

  private State state = State.OPEN;

  SrmlContext(SrmlMap<K, V> map) {
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
  public V read(K key) throws ConcurrentModeFailure {
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
      final var clonedValue = backupValue == null ? new Versioned<V>(readVersion, null) : new Versioned<>(backupValue.getVersion(), backupValue.getValue().deepClone());
      localValues.put(key, clonedValue);
      return clonedValue.getValue();
    } else if (storedValue.getVersion() <= readVersion) {
      final var clonedValue = new Versioned<>(storedValue.getVersion(), storedValue.getValue().deepClone());
      localValues.put(key, clonedValue);
      return clonedValue.getValue();
    } else {
      final var backupValue = findAmongPeers(key);
      Assert.that(backupValue != null, () -> String.format("Unable to restore value for key %s at version %d (current at %d)", key, readVersion, storedValue.getVersion()));
      final var clonedValue =  new Versioned<>(backupValue.getVersion(), backupValue.getValue().deepClone());
      localValues.put(key, clonedValue);
      return clonedValue.getValue();
    }
  }

  @Override
  public void write(K key, V value) throws ConcurrentModeFailure {
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
  public void close() throws ConcurrentModeFailure {
    if (state != State.OPEN) {
      return;
    }

    final var combinedLocks = new TreeMap<LockRef, LockModeAndState>();
    for (var read : reads) {
      final var lock = map.getLocks().forKey(read);
      if (writes.contains(read)) {
        combinedLocks.put(lock, new LockModeAndState(LockMode.WRITE));
      } else if (! combinedLocks.containsKey(lock)) {
        combinedLocks.put(lock, new LockModeAndState(LockMode.READ));
      }
    }

    for (var write : writes) {
      final var lock = map.getLocks().forKey(write);
      combinedLocks.put(lock, new LockModeAndState(LockMode.WRITE));
    }

    for (var lockEntry : combinedLocks.entrySet()) {
      final var lock = lockEntry.getKey();
      final var lockModeAndState = lockEntry.getValue();
      switch (lockModeAndState.mode) {
        case READ -> {
          try {
            lock.lock().tryReadAcquire(Long.MAX_VALUE);
          } catch (InterruptedException e) {
            rollbackFromCommitAttempt(combinedLocks);
            throw new ConcurrentModeFailure("Interrupted while acquiring read lock", e);
          }
        }
        case WRITE -> {
          try {
            lock.lock().tryWriteAcquire(Long.MAX_VALUE);
          } catch (InterruptedException e) {
            rollbackFromCommitAttempt(combinedLocks);
            throw new ConcurrentModeFailure("Interrupted while acquiring write lock", e);
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
        rollbackFromCommitAttempt(combinedLocks);
        throw new ConcurrentModeFailure("Read dependency breached for key " + read + "; expected version " + readVersion + ", saw " + storedValueVersion, null);
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

    releaseLocks(combinedLocks);
    synchronized (map.getContextLock()) {
      state = State.COMMITTED;
      map.getOpenContexts().remove(this);
      drainOpenContexts();
    }
    //    System.out.format("  %s, committed readVersion %d, writeVersion %d\n", Thread.currentThread().getName(), readVersion, writeVersion);
  }

  private void drainOpenContexts() {
    final var openContexts = map.getQueuedContexts();
    while (true) {
      final var oldest = openContexts.peekFirst();
      if (oldest != null && oldest.state != State.OPEN) {
        openContexts.removeFirst();
      } else {
        break;
      }
    }
  }

//  private void drainOpenContexts() {
//    final var openContexts = map.getQueuedContexts();
//    while (true) {
//      final var oldest = openContexts.pollFirst();
//      if (oldest == null) {
//        break;
//      } else if (oldest.state == State.OPEN) {
//        openContexts.addFirst(oldest);
//        break;
//      }
//    }
//  }

  private void releaseLocks(SortedMap<LockRef, LockModeAndState> combinedLocks) {
    for (var lockEntry : combinedLocks.entrySet()) {
      final var lock = lockEntry.getKey();
      final var lockModeAndState = lockEntry.getValue();
      if (lockModeAndState.locked) {
        switch (lockModeAndState.mode) {
          case READ -> lock.lock().readRelease();
          case WRITE -> lock.lock().writeRelease();
        }
      }
    }
  }

  private void rollbackFromCommitAttempt(SortedMap<LockRef, LockModeAndState> combinedLocks) {
    releaseLocks(combinedLocks);
    synchronized (map.getContextLock()) {
      state = State.ROLLED_BACK;
      map.getOpenContexts().remove(this);
      drainOpenContexts();
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
