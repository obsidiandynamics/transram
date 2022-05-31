package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.SrmlMap.*;
import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.spec.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

public final class Srml2Map<K, V extends DeepCloneable<V>> implements TransMap<K, V> {
  public static class Options {
    public int mutexStripes = 1024;
    public Supplier<UpgradeableMutex> mutexFactory = UnfairUpgradeableMutex::new;
  }

  public static MapFactory factory(Srml2Map.Options options) {
    return new MapFactory() {
      @Override
      public <K, V extends DeepCloneable<V>> TransMap<K, V> instantiate() {
        return new Srml2Map<>(options);
      }
    };
  }

  private final Map<K, Versioned<V>> store = new ConcurrentHashMap<>();

  private final StripedMutexes<Mutex> mutexes;

  private final Object contextLock = new Object();

  private final Set<Srml2Context<K, V>> openContexts = new CopyOnWriteArraySet<>();

  private final Deque<Srml2Context<K, V>> queuedContexts = new ConcurrentLinkedDeque<>();

  private long version;

  private final AtomicLong safeReadVersion = new AtomicLong();

  public Srml2Map(Options options) {
    mutexes = new StripedMutexes<>(options.mutexStripes, options.mutexFactory);
  }

  @Override
  public Srml2Context<K, V> transact() {
    return new Srml2Context<>(this);
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

  Set<Srml2Context<K, V>> getOpenContexts() {
    return openContexts;
  }

  Deque<Srml2Context<K, V>> getQueuedContexts() {
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
