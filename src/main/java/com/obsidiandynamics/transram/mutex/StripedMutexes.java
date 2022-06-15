package com.obsidiandynamics.transram.mutex;

import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.function.*;

public final class StripedMutexes<M extends Mutex> {
  public static final class MutexRef<M extends Mutex> implements Comparable<MutexRef<?>> {
    private final int stripe;

    private final M mutex;

    MutexRef(int stripe, M mutex) {
      this.stripe = stripe;
      this.mutex = Objects.requireNonNull(mutex);
    }

    public M mutex() {
      return mutex;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      } else if (o instanceof MutexRef) {
        final var that = (MutexRef<?>) o;
        return stripe == that.stripe;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return stripe;
    }

    @Override
    public String toString() {
      return MutexRef.class.getSimpleName() + "[stripe=" + stripe + ']';
    }

    @Override
    public int compareTo(MutexRef o) {
      return Integer.compare(stripe, o.stripe);
    }
  }

  private final MutexRef<M>[] stripes;

  @SuppressWarnings("unchecked")
  public StripedMutexes(int stripes, Supplier<UpgradeableMutex> lockFactory) {
    this.stripes = new MutexRef[stripes];
    for (var i = 0; i < stripes; i++) {
      this.stripes[i] = new MutexRef<>(i, (M) lockFactory.get());
    }
  }

  public MutexRef<M> forStripe(int stripe) {
    return stripes[stripe];
  }

  public int stripes() {
    return stripes.length;
  }

  public static int hash(Object key, int stripes) {
    return (key.hashCode() + stripes) % stripes;
  }

  public MutexRef<M> forKey(Object key) {
    return forStripe(hash(key, stripes.length));
  }

  @Override
  public String toString() {
    return StripedMutexes.class.getSimpleName() + "[stripes.length=" + stripes.length + "]";
  }
}
