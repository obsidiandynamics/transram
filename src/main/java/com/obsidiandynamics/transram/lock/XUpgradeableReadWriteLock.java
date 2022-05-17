package com.obsidiandynamics.transram.lock;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class XUpgradeableReadWriteLock {
  private final ReadWriteLock lock;

  private final Object monitor = new Object();

  private Thread upgradeCandidate;

  public XUpgradeableReadWriteLock(boolean fair) {
    lock = new ReentrantReadWriteLock(fair);
  }

  public void acquireRead() {
    lock.readLock().lock();
  }

  public boolean tryAcquireRead(long timeoutMs) throws InterruptedException {
    return lock.readLock().tryLock(timeoutMs, TimeUnit.MILLISECONDS);
  }

  public void releaseRead() {
    lock.readLock().unlock();
  }

  public void acquireWrite() {
    lock.writeLock().lock();
  }

  public boolean tryAcquireWrite(long timeoutMs) throws InterruptedException {
    return lock.writeLock().tryLock(timeoutMs, TimeUnit.MILLISECONDS);
  }

  public void upgradeWrite() throws InterruptedException {
    synchronized (monitor) {
      while (true) {
        if (upgradeCandidate == null) {
          upgradeCandidate = Thread.currentThread();
          break;
        } else {
          monitor.wait();
        }
      }
    }

    lock.readLock().unlock();
    lock.writeLock().lock();
  }

  public boolean tryUpgradeWrite(long timeoutMs) throws InterruptedException {
    final var deadline = System.currentTimeMillis() + timeoutMs;
    synchronized (monitor) {
      while (true) {
        if (upgradeCandidate == null) {
          upgradeCandidate = Thread.currentThread();
          break;
        } else {
          final var remainingTime = deadline - System.currentTimeMillis();
          if (remainingTime > 0) {
            monitor.wait(remainingTime);
          } else {
            return false;
          }
        }
      }
    }

    lock.readLock().unlock();
    final var remainingTime = deadline - System.currentTimeMillis();
    if (remainingTime > 0) {
      return lock.writeLock().tryLock(remainingTime, TimeUnit.MILLISECONDS);
    } else {
      return false;
    }
  }

  public void releaseWrite() {
    lock.writeLock().unlock();
    synchronized (monitor) {
      upgradeCandidate = null;
      monitor.notify();
    }
  }
}
