package com.obsidiandynamics.transram.lock;

import java.util.function.*;

public final class StripeLocks {
  public static final class LockRef implements Comparable<LockRef> {
    private final int stripe;

    private final UpgradeableLock lock;

    LockRef(int stripe, UpgradeableLock lock) {
      this.stripe = stripe;
      this.lock = lock;
    }

    public UpgradeableLock lock() {
      return lock;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      } else if (o instanceof LockRef) {
        final var that = (LockRef) o;
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
      return LockRef.class.getSimpleName() + "[stripe=" + stripe + ']';
    }

    @Override
    public int compareTo(LockRef o) {
      return Integer.compare(stripe, o.stripe);
    }
  }

  private final LockRef[] stripes;

  public StripeLocks(int stripes, Supplier<UpgradeableLock> lockFactory) {
    this.stripes = new LockRef[stripes];
    for (int i = 0; i < stripes; i++) {
      this.stripes[i] = new LockRef(i, lockFactory.get());
    }
  }

  public LockRef forStripe(int stripe) {
    return stripes[stripe];
  }

  private int stripeForKey(Object key) {
    final var numStripes = stripes.length;
    return (key.hashCode() + numStripes) % numStripes;
  }

  public LockRef forKey(Object key) {
    return forStripe(stripeForKey(key));
  }

  @Override
  public String toString() {
    return StripeLocks.class.getSimpleName() + "[stripes.length=" + stripes.length + "]";
  }
}
