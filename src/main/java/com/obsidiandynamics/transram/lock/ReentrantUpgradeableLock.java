package com.obsidiandynamics.transram.lock;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class ReentrantUpgradeableLock implements UpgradeableLock {
//  private static class LockState {
//    boolean writeLocked;
//  }
//
//  private final ThreadLocal<LockState> threadLocalLockState = ThreadLocal.withInitial(LockState::new);

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  private final Lock intermediateLock = new ReentrantLock();

  @Override
  public boolean tryReadAcquire(long timeoutMs) throws InterruptedException {
    return readWriteLock.readLock().tryLock(timeoutMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void readRelease() {
    readWriteLock.readLock().unlock();
  }

  @Override
  public boolean tryWriteAcquire(long timeoutMs) throws InterruptedException {
    final var intermediateLocked = intermediateLock.tryLock(timeoutMs, TimeUnit.MILLISECONDS);
    if (intermediateLocked) {
      final var writeLocked = readWriteLock.writeLock().tryLock(timeoutMs, TimeUnit.MILLISECONDS);
      if (writeLocked) {
//        final var lockState = threadLocalLockState.get();
//        lockState.writeLocked = true;
        return true;
      } else {
        intermediateLock.unlock();
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public void writeRelease() {
    readWriteLock.writeLock().unlock();
    intermediateLock.unlock();
  }

  @Override
  public boolean tryUpgrade(long timeoutMs) throws InterruptedException {
    final var intermediateLocked = intermediateLock.tryLock(timeoutMs, TimeUnit.MILLISECONDS);
    if (intermediateLocked) {
      readWriteLock.readLock().unlock();
      final var writeLocked = readWriteLock.writeLock().tryLock(timeoutMs, TimeUnit.MILLISECONDS);
      if (writeLocked) {
//        final var lockState = threadLocalLockState.get();
//        lockState.writeLocked = true;
        return true;
      } else {
        readWriteLock.readLock().lock();
        intermediateLock.unlock();
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public void downgrade() {
    readWriteLock.readLock().lock();
    readWriteLock.writeLock().unlock();
    intermediateLock.unlock();
  }
}
