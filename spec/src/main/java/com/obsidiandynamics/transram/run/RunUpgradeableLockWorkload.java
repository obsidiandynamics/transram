package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.*;

public class RunUpgradeableLockWorkload {
  private static final int NUM_THREADS = 2;

  private static final int NUM_OPS_PER_THREAD = 5_000_000;

  //  private static final double[] PROFILE = {0.25, 0.25, 0.25, 0.25};
  private static final double[] PROFILE = {0.4, 0.3, 0.2, 0.1};
  //  private static final double[] PROFILE = {0.0, 0.0, 1, 0.0};

  private static class State {
    final UpgradeableMutex lock = new UnfairUpgradeableMutex();
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
    final var workload = new Dispatcher(PROFILE);
    final var startBarrier = new CyclicBarrier(NUM_THREADS);
    for (var i = 0; i < NUM_THREADS; i++) {
      new Thread(() -> {
        try {
          startBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          throw new RuntimeException(e);
        }
        final var random = new SplittableRandom();
        try {
          for (var j = 0; j < NUM_OPS_PER_THREAD; j++) {
            final var rnd = random.nextDouble();
            workload.eval(rnd, ordinal -> {
              try {
                Opcode.values()[ordinal].operate(state);
              } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }
            });
          }
        } catch (Throwable e) {
          e.printStackTrace();
        } finally {
          latch.countDown();
        }
      }, "thread-" + i).start();
    }

    latch.await();
    final var took = System.currentTimeMillis() - startTime;
    final var stopwatches = workload.getStopwatches();
    final var expectedValue = stopwatches[Opcode.WRITE.ordinal()].getNumSamples() + stopwatches[Opcode.UPGRADE.ordinal()].getNumSamples() + stopwatches[Opcode.DOWNGRADE.ordinal()].getNumSamples();
    Assert.that(expectedValue == state.value, () -> String.format("Expected: %d, actual: %d", expectedValue, state.value));
    final int[] padding = {10, 10, 15, 15};
    System.out.format(Table.layout(padding), "opcode", "p(opcode)", "ops", "rate (op/s)");
    System.out.format(Table.layout(padding), Table.fill(padding, '-'));
    for (var opcode : Opcode.values()) {
      System.out.format(Table.layout(padding),
                        opcode,
                        String.format("%,.3f", PROFILE[opcode.ordinal()]),
                        String.format("%,d", stopwatches[opcode.ordinal()].getNumSamples()),
                        String.format("%,.0f", 1000f * stopwatches[opcode.ordinal()].getNumSamples() / took));
    }
    System.out.format(Table.layout(padding), Table.fill(padding, ' '));
    final var totalOps = Arrays.stream(stopwatches).mapToLong(Stopwatch::getNumSamples).sum();
    System.out.format(Table.layout(padding),
                      "TOTAL",
                      String.format("%,.3f", 1.0),
                      String.format("%,d", totalOps),
                      String.format("%,.0f", 1000f * totalOps / took));
  }
}
