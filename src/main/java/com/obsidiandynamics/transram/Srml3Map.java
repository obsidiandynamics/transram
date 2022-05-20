package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

public final class Srml3Map<K, V extends DeepCloneable<V>> implements TransMap<K, V> {
  public static class Options {
    public int mutexStripes = 1024;
    public Supplier<UpgradeableMutex> mutexFactory = UnfairUpgradeableMutex::new;
    public int queueDepth = 16;
  }

  private final int queueDepth;

  private final Map<K, Deque<Versioned<V>>> store = new ConcurrentHashMap<>();

  private final StripedMutexes<Mutex> mutexes;

  private final Object contextLock = new Object();

  private final Deque<Srml3Context<K, V>> queuedContexts = new ConcurrentLinkedDeque<>();

  private long version;

  private final AtomicLong safeReadVersion = new AtomicLong();

  public Srml3Map(Options options) {
    queueDepth = options.queueDepth;
    mutexes = new StripedMutexes<>(options.mutexStripes, options.mutexFactory);
  }

  int getQueueDepth() {
    return queueDepth;
  }

  @Override
  public Srml3Context<K, V> transact() {
    return new Srml3Context<>(this);
  }

  Object getContextLock() { return contextLock; }

  Map<K, Deque<Versioned<V>>> getStore() {
    return store;
  }

  @Override
  public Map<K, Versioned<V>> dirtyView() {
    final var copy = new HashMap<K, Versioned<V>>(store.size());
    for (var entry : store.entrySet()) {
      copy.put(entry.getKey(), entry.getValue().getFirst());
    }
    return copy;
  }

  StripedMutexes<Mutex> getMutexes() {
    return mutexes;
  }

  Deque<Srml3Context<K, V>> getQueuedContexts() {
    return queuedContexts;
  }

  long getVersion() {
    return version;
  }

  long incrementAndGetVersion() {
    return ++version;
  }

  AtomicLong safeReadVersion() { return safeReadVersion; }
}
