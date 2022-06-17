package com.obsidiandynamics.transram.mutex;

public interface UpgradeableMutex extends Mutex {
  boolean tryUpgrade(long timeoutMs) throws InterruptedException;
}
