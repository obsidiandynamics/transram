package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;

import java.util.concurrent.atomic.*;

public final class Failures {
  public final AtomicLong mutexFailures = new AtomicLong();
  public final AtomicLong snapshotFailures = new AtomicLong();
  public final AtomicLong antidependencyFailures = new AtomicLong();

  void classifyFailure(ConcurrentModeFailure e) {
    if (e instanceof MutexAcquisitionFailure) {
      mutexFailures.incrementAndGet();
    } else if (e instanceof BrokenSnapshotFailure) {
      snapshotFailures.incrementAndGet();
    } else if (e instanceof AntidependencyFailure) {
      antidependencyFailures.incrementAndGet();
    } else {
      throw new UnsupportedOperationException("Unsupported concurrent mode failure type " + e.getClass().getName());
    }
  }
}
