package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.spec.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

public final class SrmlMap<K, V extends DeepCloneable<V>> implements TransMap<K, V> {
  public static class Options {
    public int mutexStripes = 1024;
    public Supplier<UpgradeableMutex> mutexFactory = UnfairUpgradeableMutex::new;
    public int queueDepth = 4;
  }

  public static MapFactory factory(Options options) {
    return new MapFactory() {
      @Override
      public <K, V extends DeepCloneable<V>> TransMap<K, V> instantiate() {
        return new SrmlMap<>(options);
      }
    };
  }

  private final int queueDepth;

  private final Map<Key, Deque<Versioned<V>>> store = new ConcurrentHashMap<>();

  private final StripedMutexes<Mutex> mutexes;

  private final Object contextLock = new Object();

  private final Deque<SrmlContext<K, V>> queuedContexts = new ConcurrentLinkedDeque<>();

  private long version;

  private final AtomicLong safeReadVersion = new AtomicLong();

  public SrmlMap(Options options) {
    queueDepth = options.queueDepth;
    mutexes = new StripedMutexes<>(options.mutexStripes, options.mutexFactory);
  }

  int getQueueDepth() {
    return queueDepth;
  }

  @Override
  public SrmlContext<K, V> transact() {
    return new SrmlContext<>(this);
  }

  Object getContextLock() { return contextLock; }

  Map<Key, Deque<Versioned<V>>> getStore() {
    return store;
  }

  private final Debug<K, V> debug = new Debug<>() {
    @Override
    public Map<K, Versioned<V>> dirtyView() {
      return store.entrySet().stream()
          .filter(e -> e.getKey() instanceof  WrapperKey<?>)
          .collect(Collectors.toUnmodifiableMap(e -> Unsafe.<K>cast(((WrapperKey<?>) e.getKey()).unwrap()),
                                                e -> e.getValue().getFirst()));
    }

    @Override
    public int numRefs() {
      return store.values().stream().mapToInt(Deque::size).sum();
    }
  };

  @Override
  public Debug<K, V> debug() { return debug; }

  StripedMutexes<Mutex> getMutexes() {
    return mutexes;
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

  AtomicLong safeReadVersion() { return safeReadVersion; }
}
