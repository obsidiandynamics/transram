package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

public final class Ss2plMap<K, V extends DeepCloneable<V>> implements TransMap<K, V> {
  public static class Options {
    public int mutexStripes = 1024;
    public Supplier<UpgradeableMutex> mutexFactory = UnfairUpgradeableMutex::new;
    public long mutexTimeoutMs = 10;
  }

  private final Options options;

  private final Map<K, Versioned<V>> store = new ConcurrentHashMap<>();

  private final StripedMutexes<UpgradeableMutex> mutexes;

  private final AtomicLong version = new AtomicLong();

  public Ss2plMap(Options options) {
    this.options = options;
    mutexes = new StripedMutexes<>(options.mutexStripes, options.mutexFactory);
  }

  @Override
  public Ss2plContext<K, V> transact() {
    return new Ss2plContext<>(this, options.mutexTimeoutMs);
  }

  Map<K, Versioned<V>> getStore() {
    return store;
  }

  @Override
  public Map<K, Versioned<V>> dirtyView() {
    return Map.copyOf(store);
  }

  StripedMutexes<UpgradeableMutex> getMutexes() {
    return mutexes;
  }

  AtomicLong version() {
    return version;
  }
}
