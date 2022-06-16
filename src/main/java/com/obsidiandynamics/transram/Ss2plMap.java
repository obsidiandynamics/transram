package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.spec.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

public final class Ss2plMap<K, V extends DeepCloneable<V>> implements TransMap<K, V> {
  public static class Options {
    public int mutexStripes = 1024;
    public Supplier<UpgradeableMutex> mutexFactory = UnfairUpgradeableMutex::new;
    public long mutexTimeoutMs = 10;

    void validate() {
      Assert.that(mutexStripes > 0, () -> "Number of mutex stripes must exceed 0");
      Assert.that(mutexTimeoutMs > 0, () -> "Mutex timeout must be equal to or greater than 0");
    }
  }

  public static MapFactory factory(Ss2plMap.Options options) {
    return new MapFactory() {
      @Override
      public <K, V extends DeepCloneable<V>> TransMap<K, V> instantiate() {
        return new Ss2plMap<>(options);
      }
    };
  }

  private final Options options;

  private final Map<Key, RawVersioned> store = new ConcurrentHashMap<>();

  private final StripedMutexes<UpgradeableMutex> mutexes;

  private final AtomicLong version = new AtomicLong();

  public Ss2plMap(Options options) {
    options.validate();
    this.options = options;
    mutexes = new StripedMutexes<>(options.mutexStripes, options.mutexFactory);
    store.put(InternalKey.SIZE, new RawVersioned(0, new Size(0)));
  }

  @Override
  public Ss2plContext<K, V> transact() {
    return new Ss2plContext<>(this, options.mutexTimeoutMs);
  }

  Map<Key, RawVersioned> getStore() {
    return store;
  }

  private final Debug<K, V> debug = new Debug<>() {
    @Override
    public Map<K, GenericVersioned<V>> dirtyView() {
      return store.entrySet().stream()
          .filter(e -> e.getKey() instanceof KeyRef<?>)
          .collect(Collectors.toUnmodifiableMap(e -> Unsafe.<K>cast(((KeyRef<?>) e.getKey()).unwrap()),
                                                e -> e.getValue().generify()));
    }

    @Override
    public int numRefs() {
      return store.size();
    }

    @Override
    public long getVersion() {
      return version.get();
    }
  };

  @Override
  public Debug<K, V> debug() { return debug; }

  StripedMutexes<UpgradeableMutex> getMutexes() {
    return mutexes;
  }

  AtomicLong version() {
    return version;
  }
}
