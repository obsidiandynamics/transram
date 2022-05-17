package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.lock.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static com.obsidiandynamics.transram.util.Table.*;

public class RunUpgradeableLockWorkload {
  private static final int NUM_THREADS = 2;

  private static final int NUM_OPS_PER_THREAD = 5_000_000;

  //  private static final double[] PROFILE = {0.25, 0.25, 0.25, 0.25};
  private static final double[] PROFILE = {0.4, 0.3, 0.2, 0.1};
  //  private static final double[] PROFILE = {0.0, 0.0, 1, 0.0};

  private static class State {
    final UpgradeableLock lock = new UnfairUpgradeableLock();
    long value;
    final Object upgradeGuard = new Object();
  }

  private enum Opcode {
    READ {
      @Override
      void operate(State state) throws InterruptedException {
        Assert.that(state.lock.tryReadAcquire(Long.MAX_VALUE));
        state.lock.readRelease();
      }
    },
    WRITE {
      @Override
      void operate(State state) throws InterruptedException {
        Assert.that(state.lock.tryWriteAcquire(Long.MAX_VALUE));
        state.value += 1;
        state.lock.writeRelease();
      }
    },
    UPGRADE {
      @Override
      void operate(State state) throws InterruptedException {
        synchronized (state.upgradeGuard) {
          Assert.that(state.lock.tryReadAcquire(Long.MAX_VALUE));
          final var afterRead = state.value;
          Assert.that(state.lock.tryUpgrade(Long.MAX_VALUE));
          Assert.that(afterRead == state.value, () -> String.format("Expected %d, got %d", afterRead, state.value));
          state.value = afterRead + 1;
          state.lock.writeRelease();
        }
      }
    },
    DOWNGRADE {
      @Override
      void operate(State state) throws InterruptedException {
        Assert.that(state.lock.tryWriteAcquire(Long.MAX_VALUE));
        final var beforeWrite = state.value;
        final var afterWrite = beforeWrite + 1;
        state.value = afterWrite;
        state.lock.downgrade();
        Assert.that(state.value == afterWrite, () -> String.format("Expected %d, got %d", afterWrite, state.value));
        state.lock.readRelease();
      }
    };

    abstract void operate(State state) throws InterruptedException;
  }

  public static void main(String[] args) throws InterruptedException {
    final var state = new State();
    final var latch = new CountDownLatch(NUM_THREADS);
    final var startTime = System.currentTimeMillis();
    final var workload = new Workload(PROFILE);
    for (int i = 0; i < NUM_THREADS; i++) {
      new Thread(() -> {
        final var random = new SplittableRandom();
        try {
          for (int j = 0; j < NUM_OPS_PER_THREAD; j++) {
            final var rnd = random.nextDouble();
            final var opcode = Opcode.values()[workload.eval(rnd)];
            opcode.operate(state);
//            if (false) throw new InterruptedException("");
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
    final var counters = workload.getCounters();
    final var expectedValue = counters[Opcode.WRITE.ordinal()].get() + counters[Opcode.UPGRADE.ordinal()].get() + counters[Opcode.DOWNGRADE.ordinal()].get();
    Assert.that(expectedValue == state.value, () -> String.format("Expected: %d, actual: %d", expectedValue, state.value));
    final int[] padding = {10, 10, 15, 15};
    System.out.format(layout(padding), "opcode", "p(opcode)", "ops", "rate (op/s)");
    System.out.format(layout(padding), fill(padding, '-'));
    for (var opcode : Opcode.values()) {
      System.out.format(layout(padding),
                        opcode,
                        String.format("%,.3f", PROFILE[opcode.ordinal()]),
                        String.format("%,d", counters[opcode.ordinal()].get()),
                        String.format("%,.0f", 1000f * counters[opcode.ordinal()].get() / took));
    }
    System.out.format(layout(padding), fill(padding, ' '));
    final var totalOps = Arrays.stream(counters).mapToLong(AtomicLong::get).sum();
    System.out.format(layout(padding),
                      "TOTAL",
                      String.format("%,.3f", 1.0),
                      String.format("%,d", totalOps),
                      String.format("%,.0f", 1000f * totalOps / took));
  }
}
