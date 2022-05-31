package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;

import java.util.concurrent.atomic.*;

public final class Failures {
  public final AtomicLong mutex = new AtomicLong();
  public final AtomicLong snapshot = new AtomicLong();
  public final AtomicLong antidependency = new AtomicLong();

  public void increment(ConcurrentModeFailure e) {
    if (e instanceof MutexAcquisitionFailure) {
      mutex.incrementAndGet();
    } else if (e instanceof BrokenSnapshotFailure) {
      snapshot.incrementAndGet();
    } else if (e instanceof AntidependencyFailure) {
      antidependency.incrementAndGet();
    } else {
      throw new UnsupportedOperationException("Unsupported concurrent mode failure type " + e.getClass().getName());
    }
  }
}
