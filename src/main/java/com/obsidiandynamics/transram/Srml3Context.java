package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.TransContext.*;
import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.mutex.StripedMutexes.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class Srml3Context<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final Srml3Map<K, V> map;

  private final Set<K> reads = new HashSet<>();

  private final Set<K> writes = new HashSet<>();

  private final Map<K, Versioned<V>> localValues = new HashMap<>();

  private final long readVersion;

  private long writeVersion = -1;

  private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);

  Srml3Context(Srml3Map<K, V> map) {
    this.map = map;

    readVersion = map.safeReadVersion().get();
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

    final var storedValues = map.getStore().get(key);
    if (storedValues == null) {
      final var nullValue = new Versioned<V>(readVersion, null);
      localValues.put(key, nullValue);
      return nullValue.getValue();
    } else {
      for (var storedValue : storedValues) {
        if (storedValue.getVersion() <= readVersion) {
          final var clonedValue = storedValue.deepClone();
          localValues.put(key, clonedValue);
          return clonedValue.getValue();
        }
      }

      throw new BrokenSnapshotFailure("Unable to restore value for key " + key + " at version " + readVersion + ", current at " + storedValues.getFirst().getVersion());
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

    synchronized (map.getContextLock()) {
      writeVersion = map.incrementAndGetVersion();
      map.getQueuedContexts().addLast(this);
    }

    for (var write : writes) {
      final var replacementValue = new Versioned<>(writeVersion, localValues.get(write).getValue());
       map.getStore().compute(write, (__, previousValues) -> {
         if (previousValues == null) {
           final var newValues = new ConcurrentLinkedDeque<Versioned<V>>();
           newValues.add(replacementValue);
           return newValues;
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
}
