package com.obsidiandynamics.transram.mutex;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public final class ReentrantMutex implements Mutex {
  private final ReadWriteLock lock;

  public ReentrantMutex(boolean fair) {
    lock = new ReentrantReadWriteLock(fair);
  }

  @Override
  public boolean tryReadAcquire(long timeoutMs) throws InterruptedException {
    return lock.readLock().tryLock(timeoutMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void readRelease() {
    lock.readLock().unlock();
  }

  @Override
  public boolean tryWriteAcquire(long timeoutMs) throws InterruptedException {
    return lock.writeLock().tryLock(timeoutMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void writeRelease() {
    lock.writeLock().unlock();
  }

  @Override
  public void downgrade() {
    lock.readLock().lock();
    lock.writeLock().unlock();
  }
}
