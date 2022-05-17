package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.lock.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

public class Ss2plMap<K, V extends DeepCloneable<V>> implements TransMap<K, V> {
  public static class Options {
    public int lockStripes = 1024;
    public Supplier<UpgradeableLock> lockFactory = UnfairUpgradeableLock::new;
    public long lockTimeoutMs = 10;
  }

  private final Options options;

  private final Map<K, Versioned<V>> store = new ConcurrentHashMap<>();

  private final StripeLocks locks;

  private final AtomicLong version = new AtomicLong();

  public Ss2plMap(Options options) {
    this.options = options;
    locks = new StripeLocks(options.lockStripes, options.lockFactory);
  }

  @Override
  public Ss2plContext<K, V> transact() {
    return new Ss2plContext<>(this, options.lockTimeoutMs);
  }

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

  AtomicLong version() {
    return version;
  }
}
