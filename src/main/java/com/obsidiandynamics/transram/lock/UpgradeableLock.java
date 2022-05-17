package com.obsidiandynamics.transram.lock;

public interface UpgradeableLock {
  boolean tryReadAcquire(long timeoutMs) throws InterruptedException;

  void readRelease();

  boolean tryWriteAcquire(long timeoutMs) throws InterruptedException;

  void writeRelease();

  boolean tryUpgrade(long timeoutMs) throws InterruptedException;

  void downgrade();
}
