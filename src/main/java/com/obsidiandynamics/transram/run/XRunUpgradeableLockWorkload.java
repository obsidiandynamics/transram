package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.lock.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static com.obsidiandynamics.transram.util.Table.*;

public class XRunUpgradeableLockWorkload {
  private static final int NUM_THREADS = 2;

  private static final int NUM_OPS_PER_THREAD = 5_000_000;

  private enum Opcode {
    READ,
    WRITE,
    UPGRADE,
    DOWNGRADE
  }

//  private static final double[] PROFILE = {0.25, 0.25, 0.25, 0.25};
  private static final double[] PROFILE = {0.4, 0.3, 0.2, 0.1};
//  private static final double[] PROFILE = {0.0, 0.0, 1, 0.0};
  private static class Counter {
    final UpgradeableLock lock = new UnfairUpgradeableLock();
//    final UpgradeableLock lock = new ReentrantUpgradeableLock();
    long value;
    final Object upgradeGuard = new Object();

    void read() throws InterruptedException {
      Assert.that(lock.tryReadAcquire(Long.MAX_VALUE));
      lock.readRelease();
    }

    void write() throws InterruptedException {
      Assert.that(lock.tryWriteAcquire(Long.MAX_VALUE));
      value += 1;
      lock.writeRelease();
    }

    void upgrade() throws InterruptedException {
      synchronized (upgradeGuard) {
        Assert.that(lock.tryReadAcquire(Long.MAX_VALUE));
        final var afterRead = value;
        Assert.that(lock.tryUpgrade(Long.MAX_VALUE));
        Assert.that(afterRead == value, () -> String.format("Expected %d, got %d", afterRead, value));
        value = afterRead + 1;
        lock.writeRelease();
      }
    }

    void downgrade() throws InterruptedException {
      Assert.that(lock.tryWriteAcquire(Long.MAX_VALUE));
      final var beforeWrite = value;
      final var afterWrite = beforeWrite + 1;
      value = afterWrite;
      lock.downgrade();
      Assert.that(value == afterWrite, () -> String.format("Expected %d, got %d", afterWrite, value));
      lock.readRelease();
    }
  }

  public static void main(String[] args) throws InterruptedException {
    Assert.that(Math.abs(Arrays.stream(PROFILE).sum() - 1) < Double.MIN_VALUE, () -> "Ensure probabilities add to 1");
    final var counter = new Counter();
    final var maxProbRead = PROFILE[Opcode.READ.ordinal()];
    final var maxProbWrite = PROFILE[Opcode.WRITE.ordinal()] + maxProbRead;
    final var maxProbUpgrade = PROFILE[Opcode.UPGRADE.ordinal()] + maxProbWrite;
    final var reads = new AtomicLong();
    final var writes = new AtomicLong();
    final var upgrades = new AtomicLong();
    final var downgrades = new AtomicLong();
    final var opCounters = new AtomicLong[]{reads, writes, upgrades, downgrades};

    final var latch = new CountDownLatch(NUM_THREADS);
    final var startTime = System.currentTimeMillis();
    for (int i = 0; i < NUM_THREADS; i++) {
      new Thread(() -> {
        final var random = new SplittableRandom();
        try {
          for (int j = 0; j < NUM_OPS_PER_THREAD; j++) {
            final var rnd= random.nextDouble();
            if (rnd > maxProbUpgrade) {
              counter.downgrade();
              downgrades.incrementAndGet();
            } else if (rnd > maxProbWrite && rnd <= maxProbUpgrade) {
              counter.upgrade();
              upgrades.incrementAndGet();
            } else if (rnd > maxProbRead && rnd <= maxProbWrite) {
              counter.write();
              writes.incrementAndGet();
            } else {
              counter.read();
              reads.incrementAndGet();
            }
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (Throwable e) {
          e.printStackTrace();
        } finally {
          latch.countDown();
        }
      }, "thread-" + i).start();
    }

    latch.await();
    final var took = System.currentTimeMillis() - startTime;
    final var expectedValue = writes.get() + upgrades.get() + downgrades.get();
    Assert.that(expectedValue == counter.value, () -> String.format("Expected: %d, actual: %d", expectedValue, counter.value));
    final int[] padding = {10, 10, 15, 15};
    System.out.format(layout(padding), "opcode", "p(opcode)", "ops", "rate (op/s)");
    System.out.format(layout(padding), fill(padding, '-'));
    for (var opcode : Opcode.values()) {
      System.out.format(layout(padding),
                        opcode,
                        String.format("%,.3f", PROFILE[opcode.ordinal()]),
                        String.format("%,d", opCounters[opcode.ordinal()].get()),
                        String.format("%,.0f", 1000f * opCounters[opcode.ordinal()].get() / took));
    }
    System.out.format(layout(padding), fill(padding, ' '));
    final var totalOps = reads.get() + writes.get() + upgrades.get() + downgrades.get();
    System.out.format(layout(padding),
                      "TOTAL",
                      String.format("%,.3f", 1.0),
                      String.format("%,d", totalOps),
                      String.format("%,.0f", 1000f * totalOps / took));
  }
}
