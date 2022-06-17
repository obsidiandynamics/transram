package com.obsidiandynamics.transram.mutex;

public final class UnfairUpgradeableMutex implements UpgradeableMutex {
  private static class LockState {
    boolean readLocked, writeLocked;
  }

  private final Object monitor = new Object();

  private final ThreadLocal<LockState> threadLocalLockState = ThreadLocal.withInitial(LockState::new);

  private int readers;

  private boolean writeLocked;

  @Override
  public boolean tryReadAcquire(long timeoutMs) throws InterruptedException {
    final var lockState = threadLocalLockState.get();
    if (lockState.readLocked) {
      throw new IllegalMonitorStateException("Already read-locked");
    }

    if (lockState.writeLocked) {
      throw new IllegalMonitorStateException("Already write-locked, use downgrade method");
    }

    var deadline = 0L;
    synchronized (monitor) {
      while (true) {
        if (!writeLocked) {
          readers++;
          lockState.readLocked = true;
          return true;
        } else if (timeoutMs > 0) {
          final var currentTime = System.currentTimeMillis();
          if (deadline == 0) {
            deadline = addNoWrap(currentTime, timeoutMs);
          }
          final var remaining = deadline - currentTime;
          if (remaining > 0) {
            monitor.wait(remaining);
          } else {
            return false;
          }
        } else {
          return false;
        }
      }
    }
  }

  @Override
  public void readRelease() {
    final var lockState = threadLocalLockState.get();
    if (!lockState.readLocked) {
      throw new IllegalMonitorStateException("Not read-locked");
    }

    synchronized (monitor) {
      readers--;
      lockState.readLocked = false;
      if (readers == 1) {
        monitor.notifyAll(); // in case of a pending upgrade
      } else if (readers == 0) {
        monitor.notify();
      }
    }
  }

  @Override
  public boolean tryWriteAcquire(long timeoutMs) throws InterruptedException {
    final var lockState = threadLocalLockState.get();
    if (lockState.writeLocked) {
      throw new IllegalMonitorStateException("Already write-locked");
    }

    if (lockState.readLocked) {
      throw new IllegalMonitorStateException("Already read-locked, use upgrade methods");
    }

    var deadline = 0L;
    synchronized (monitor) {
      while (true) {
        if (!writeLocked && readers == 0) {
          writeLocked = true;
          lockState.writeLocked = true;
          return true;
        } else if (timeoutMs > 0) {
          final var currentTime = System.currentTimeMillis();
          if (deadline == 0) {
            deadline = addNoWrap(currentTime, timeoutMs);
          }
          final var remaining = deadline - currentTime;
          if (remaining > 0) {
            monitor.wait(remaining);
          } else {
            return false;
          }
        } else {
          return false;
        }
      }
    }
  }

  @Override
  public void writeRelease() {
    final var lockState = threadLocalLockState.get();
    if (!lockState.writeLocked) {
      throw new IllegalMonitorStateException("Not write-locked");
    }

    synchronized (monitor) {
      writeLocked = false;
      lockState.writeLocked = false;
      monitor.notify();
    }
  }

  @Override
  public boolean tryUpgrade(long timeoutMs) throws InterruptedException {
    final var lockState = threadLocalLockState.get();
    if (lockState.writeLocked) {
      throw new IllegalMonitorStateException("Already write-locked");
    }

    if (!lockState.readLocked) {
      throw new IllegalMonitorStateException("Not read-locked, cannot upgrade");
    }

    var deadline = 0L;
    synchronized (monitor) {
      while (true) {
        if (readers == 1) {
          writeLocked = true;
          readers = 0;
          lockState.readLocked = false;
          lockState.writeLocked = true;
          return true;
        } else if (timeoutMs > 0) {
          final var currentTime = System.currentTimeMillis();
          if (deadline == 0) {
            deadline = addNoWrap(currentTime, timeoutMs);
          }
          final var remaining = deadline - currentTime;
          if (remaining > 0) {
            monitor.wait(remaining);
          } else {
            return false;
          }
        } else {
          return false;
        }
      }
    }
  }

  @Override
  public void downgrade() {
    final var lockState = threadLocalLockState.get();
    if (!lockState.writeLocked) {
      throw new IllegalMonitorStateException("Not write-locked");
    }

    if (lockState.readLocked) {
      throw new IllegalMonitorStateException("Already read-locked, cannot downgrade");
    }

    synchronized (monitor) {
      readers = 1;
      writeLocked = false;
      lockState.readLocked = true;
      lockState.writeLocked = false;
      monitor.notifyAll(); // in case of a pending reader
    }
  }

  private static long addNoWrap(long l1, long l2) {
    final var sum = l1 + l2;
    return sum < 0 ? Long.MAX_VALUE : sum;
  }
}
