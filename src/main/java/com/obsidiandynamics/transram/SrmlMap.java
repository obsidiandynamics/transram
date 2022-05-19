package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

public final class SrmlMap<K, V extends DeepCloneable<V>> implements TransMap<K, V> {
  public static class Options {
    public int mutexStripes = 1024;
    public Supplier<UpgradeableMutex> mutexFactory = UnfairUpgradeableMutex::new;
  }

  private final Map<K, Versioned<V>> store = new ConcurrentHashMap<>();

  private final StripedMutexes<Mutex> mutexes;

  private final Object contextLock = new Object();

  private final Set<SrmlContext<K, V>> openContexts = new HashSet<>();

  private final Deque<SrmlContext<K, V>> queuedContexts = new LinkedList<>();

  private long version;

  public SrmlMap(Options options) {
    mutexes = new StripedMutexes<>(options.mutexStripes, options.mutexFactory);
  }

  @Override
  public SrmlContext<K, V> transact() {
    return new SrmlContext<>(this);
  }

  Object getContextLock() { return contextLock; }

  Map<K, Versioned<V>> getStore() {
    return store;
  }

  @Override
  public Map<K, Versioned<V>> dirtyView() {
    return Map.copyOf(store);
  }

  StripedMutexes<Mutex> getMutexes() {
    return mutexes;
  }

  Set<SrmlContext<K, V>> getOpenContexts() {
    return openContexts;
  }

  Deque<SrmlContext<K, V>> getQueuedContexts() {
    return queuedContexts;
  }

  long getVersion() {
    return version;
  }

  long incrementAndGetVersion() {
    return ++version;
  }
}
