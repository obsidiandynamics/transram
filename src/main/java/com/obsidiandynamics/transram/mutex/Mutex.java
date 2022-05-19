package com.obsidiandynamics.transram.mutex;

public interface Mutex {
  boolean tryReadAcquire(long timeoutMs) throws InterruptedException;

  void readRelease();

  boolean tryWriteAcquire(long timeoutMs) throws InterruptedException;

  void writeRelease();

  void downgrade();
}
