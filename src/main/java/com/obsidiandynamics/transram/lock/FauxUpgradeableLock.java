package com.obsidiandynamics.transram.lock;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class FauxUpgradeableLock implements UpgradeableLock {
  private final ReadWriteLock lock = new ReentrantReadWriteLock(false);

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
  public boolean tryUpgrade(long timeoutMs) throws InterruptedException {
    lock.readLock().unlock();
    final var writeLocked = lock.writeLock().tryLock(timeoutMs, TimeUnit.MILLISECONDS);
    if (! writeLocked) {
      lock.readLock().lock();
    }
    return writeLocked;
  }

  @Override
  public void downgrade() {
    lock.readLock().lock();
    lock.writeLock().unlock();
  }
}
