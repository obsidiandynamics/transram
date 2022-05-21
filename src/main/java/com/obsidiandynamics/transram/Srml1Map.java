package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

public final class Srml1Map<K, V extends DeepCloneable<V>> implements TransMap<K, V> {
  public static class Options {
    public int mutexStripes = 1024;
    public Supplier<UpgradeableMutex> mutexFactory = UnfairUpgradeableMutex::new;
  }

  private final Map<K, Versioned<V>> store = new ConcurrentHashMap<>();

  private final StripedMutexes<Mutex> mutexes;

  private final Object contextLock = new Object();

  private final Set<Srml1Context<K, V>> openContexts = new HashSet<>();

  private final Deque<Srml1Context<K, V>> queuedContexts = new LinkedList<>();

  private long version;

  public Srml1Map(Options options) {
    mutexes = new StripedMutexes<>(options.mutexStripes, options.mutexFactory);
  }

  @Override
  public Srml1Context<K, V> transact() {
    return new Srml1Context<>(this);
  }

  Object getContextLock() { return contextLock; }

  Map<K, Versioned<V>> getStore() {
    return store;
  }

  private final Debug<K, V> debug = new Debug<>() {
    @Override
    public Map<K, Versioned<V>> dirtyView() {
      return Map.copyOf(store);
    }

    @Override
    public int numRefs() {
      return store.size();
    }
  };

  @Override
  public Debug<K, V> debug() { return debug; }

  StripedMutexes<Mutex> getMutexes() {
    return mutexes;
  }

  Set<Srml1Context<K, V>> getOpenContexts() {
    return openContexts;
  }

  Deque<Srml1Context<K, V>> getQueuedContexts() {
    return queuedContexts;
  }

  long getVersion() {
    return version;
  }

  long incrementAndGetVersion() {
    return ++version;
  }
}
