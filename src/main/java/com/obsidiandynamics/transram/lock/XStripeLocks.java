package com.obsidiandynamics.transram.lock;

import java.util.concurrent.locks.*;

public final class XStripeLocks {
  public static final class LockRef {
    private final int stripe;

    private final ReadWriteLock lock;

    LockRef(int stripe, ReadWriteLock lock) {
      this.stripe = stripe;
      this.lock = lock;
    }

    public Lock readLock() {
      return lock.readLock();
    }

    public Lock writeLock() {
      return lock.writeLock();
    }

    public String toString() {
      return LockRef.class.getSimpleName() + "[stripe=" + stripe + ']';
    }
  }

  private final LockRef[] stripes;

  public XStripeLocks(int stripes, boolean fair) {
    this.stripes = new LockRef[stripes];
    for (int i = 0; i < stripes; i++) {
      this.stripes[i] = new LockRef(i, new ReentrantReadWriteLock(fair));
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
    return XStripeLocks.class.getSimpleName() + "[stripes.length=" + stripes.length + "]";
  }
}
