package com.obsidiandynamics.transram.mutex;

import java.util.concurrent.*;

final class ThreadedUpgradeableMutex extends ThreadedMutex implements UpgradeableMutex {
  ThreadedUpgradeableMutex(UpgradeableMutex delegate, ExecutorService executor) {
    super(delegate, executor);
  }

  @Override
  public boolean tryUpgrade(long timeoutMs) throws InterruptedException {
    return tryUpgradeAsync(timeoutMs).get();
  }

  public MutexFuture tryUpgradeAsync(long timeoutMs) {
    return submit(() -> ((UpgradeableMutex) delegate).tryUpgrade(timeoutMs));
  }
}
