package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.lock.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

public class SrmlMap<K, V extends DeepCloneable<V>> implements TransMap<K, V> {
  public static class Options {
    public int lockStripes = 1024;
    public Supplier<UpgradeableLock> lockFactory = UnfairUpgradeableLock::new;
  }

  private final Options options;

  private final Map<K, Versioned<V>> store = new ConcurrentHashMap<>();

  private final StripeLocks locks;

  private final Object contextLock = new Object();

  private final Set<SrmlContext<K, V>> openContexts = new HashSet<>();

  private final Deque<SrmlContext<K, V>> queuedContexts = new LinkedList<>();

  private long version;

  public SrmlMap(Options options) {
    this.options = options;
    locks = new StripeLocks(options.lockStripes, options.lockFactory);
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

  StripeLocks getLocks() {
    return locks;
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
