package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.Enclose.Region.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import static com.obsidiandynamics.transram.util.Table.*;

public final class Harness {
  private static class RunOptions implements Cloneable {
    int thinkTimeMs = -1;
    int numAccounts;
    int initialBalance;
    int maxXferAmount;
    int scanAccounts;
    int numThreads;
    int numOpsPerThread;
    boolean log = false;

    void validate() {
      Assert.that(thinkTimeMs >= 0);
      Assert.that(numAccounts > 0);
      Assert.that(initialBalance > 0);
      Assert.that(maxXferAmount > 0);
      Assert.that(scanAccounts > 0);
      Assert.that(numThreads > 0);
      Assert.that(numOpsPerThread > 0);
    }

    @Override
    public RunOptions clone() {
      try {
        return (RunOptions) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new UnsupportedOperationException(e);
      }
    }
  }

  private static final RunOptions RUN_OPTIONS = new RunOptions() {{
    thinkTimeMs = 0;
    numAccounts = 1_000;
    initialBalance = 100;
    maxXferAmount = 100;
    scanAccounts = 100;
    numThreads = 16;
    numOpsPerThread = 1_000;
  }};

  private static final long MIN_DURATION_MS = 5_000;

  private static final double WARMUP_FRACTION = 0.1;

  private static final double[][] PROFILES = {
      {0.1, 0.0, 0.9},
      {0.5, 0.0, 0.5},
      {0.9, 0.0, 0.1}
  };

  private static class State {
    final TransMap<Integer, Account> map;
    final AtomicLong mutexFailures = new AtomicLong();
    final AtomicLong snapshotFailures = new AtomicLong();
    final AtomicLong antidependencyFailures = new AtomicLong();

    State(TransMap<Integer, Account> map) {
      this.map = map;
    }

    void classifyFailure(ConcurrentModeFailure e) {
      if (e instanceof MutexAcquisitionFailure) {
        mutexFailures.incrementAndGet();
      } else if (e instanceof BrokenSnapshotFailure) {
        snapshotFailures.incrementAndGet();
      } else if (e instanceof AntidependencyFailure) {
        antidependencyFailures.incrementAndGet();
      } else {
        throw new UnsupportedOperationException("Unsupported concurrent mode failure type " + e.getClass().getName());
      }
    }
  }

  private enum Opcode {
    SNAPSHOT_READ {
      @Override
      void operate(State state, SplittableRandom rnd, RunOptions options) {
        final var firstAccountId = (int) (rnd.nextDouble() * options.numAccounts);
        Enclose.over(state.map)
            .onFailure(state::classifyFailure)
            .transact(ctx -> {
              for (var i = 0; i < options.scanAccounts; i++) {
                final var accountId = i + firstAccountId;
                final var account = ctx.read(accountId % options.numAccounts);
                Assert.that(account != null, () -> String.format("Account %d was null", accountId));
              }
              think(options.thinkTimeMs);
              return Action.ROLLBACK;
            });
      }
    },

    READ_ONLY {
      @Override
      void operate(State state, SplittableRandom rnd, RunOptions options) {
        final var firstAccountId = (int) (rnd.nextDouble() * options.numAccounts);
        Enclose.over(state.map)
            .onFailure(state::classifyFailure)
            .transact(ctx -> {
              for (var i = 0; i < options.scanAccounts; i++) {
                final var accountId = i + firstAccountId;
                final var account = ctx.read(accountId % options.numAccounts);
                Assert.that(account != null, () -> String.format("Account %d was null", accountId));
              }
              think(options.thinkTimeMs);
              return Action.COMMIT;
            });
      }
    },

    XFER {
      @Override
      void operate(State state, SplittableRandom rnd, RunOptions options) {
        Enclose.over(state.map)
            .onFailure(state::classifyFailure)
            .transact(ctx -> {
              final var fromAccountId = (int) (rnd.nextDouble() * options.numAccounts);
              final var toAccountId = (int) (rnd.nextDouble() * options.numAccounts);
              final var amount = 1 + (int) (rnd.nextDouble() * (options.maxXferAmount - 1));
              if (toAccountId == fromAccountId) {
                return Action.ROLLBACK_AND_RESET;
              }
              if (options.log) System.out.format("%s, fromAccountId=%d, toAccountId=%d, amount=%d\n", Thread.currentThread().getName(), fromAccountId, toAccountId, amount);
              final var fromAccount = ctx.read(fromAccountId);
              final var toAccount = ctx.read(toAccountId);

              Assert.that(fromAccount != null, () -> String.format("Account (from) %d is null", fromAccountId));
              final var newFromBalance = fromAccount.getBalance() - amount;
              if (newFromBalance < 0) {
                return Action.ROLLBACK_AND_RESET;
              }

              fromAccount.setBalance(newFromBalance);
              Assert.that(toAccount != null, () -> String.format("Account (to) %d is null", toAccountId));
              toAccount.setBalance(toAccount.getBalance() + amount);

              ctx.write(fromAccountId, fromAccount);
              ctx.write(toAccountId, toAccount);
              think(options.thinkTimeMs);
              return Action.COMMIT;
            });
      }
    };

    abstract void operate(State state, SplittableRandom rnd, RunOptions options);

    private static void think(long time) {
      if (time > 0) {
        try {
          Thread.sleep(time);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public static void run(Supplier<TransMap<Integer, Account>> mapFactory) throws InterruptedException {
    System.out.format("Running benchmarks for %s...\n",  mapFactory.get().getClass().getSimpleName());
    System.out.format("- Warmup...\n");
    final var warmupOptions = RUN_OPTIONS.clone();
    warmupOptions.numOpsPerThread *= WARMUP_FRACTION;
    final var warmupProfile = new double[]{0.33, 0.33, 0.34};
    runOne(mapFactory, warmupOptions, warmupProfile, (long) (MIN_DURATION_MS * WARMUP_FRACTION));

    final var results = new RunResult[PROFILES.length];
    for (var i = 0; i < PROFILES.length; i++) {
      System.out.format("- Workload %d of %d: %s...\n", i + 1, PROFILES.length, Arrays.toString(PROFILES[i]));
      final var result = runOne(mapFactory, RUN_OPTIONS, PROFILES[i], MIN_DURATION_MS);
      dumpDetail(result, PROFILES[i]);
      System.out.println();
      results[i] = result;
    }

    System.out.println();
    System.out.format("- Summary:\n");
    dumpSummaries(results, PROFILES);
  }

  private static class RunResult {
    final long elapsedMs;
    final Workload workload;
    final State state;

    RunResult(long elapsedMs, Workload workload, State state) {
      this.elapsedMs = elapsedMs;
      this.workload = workload;
      this.state = state;
    }
  }

  private static RunResult runOne(Supplier<TransMap<Integer, Account>> mapFactory, RunOptions options, double[] profile, long minDurationMs) throws InterruptedException {
    final var map = mapFactory.get();
    final var state = new State(map);

    // initialise bank accounts
    for (var i = 0; i < options.numAccounts; i++) {
      final var accountId = i;
      Enclose.over(state.map).transact(ctx -> {
        final var existingAccount = ctx.read(accountId);
        Assert.that(existingAccount == null, () -> String.format("Found existing account %d (%s)", accountId, existingAccount));
        ctx.write(accountId, new Account(accountId, options.initialBalance));
        return Action.COMMIT;
      });
    }

    final var workload = new Workload(profile);
    final var latch = new CountDownLatch(options.numThreads);
    final var startTime = System.currentTimeMillis();
    for (var i = 0; i < options.numThreads; i++) {
      new Thread(() -> {
        final var rnd = new SplittableRandom();
        var opsPerThread = options.numOpsPerThread;
        var op = 0;
        while (true) {
          final var opcode = Opcode.values()[workload.eval(rnd.nextDouble())];
          opcode.operate(state, rnd, options);
          if (++op == opsPerThread) {
            final var took = System.currentTimeMillis() - startTime;
            if (took < minDurationMs) {
              final var targetOpsPerThread = (long) ((double) opsPerThread * minDurationMs / took);
              opsPerThread += Math.max(1, (targetOpsPerThread - opsPerThread) / 2);
            } else {
              break;
            }
          }
        }
        latch.countDown();
      }, "xfer_thread_" + i).start();
    }

    latch.await();
    final var took = System.currentTimeMillis() - startTime;
    if (options.log) dumpMap(state.map);
    checkMapSum(state.map, options);

    return new RunResult(took, workload, state);
  }

  private static void dumpDetail(RunResult result, double[] profile) {
    final int[] padding = {15, 10, 15, 15};
    System.out.format(layout(padding), "opcode", "p(opcode)", "ops", "rate (op/s)");
    System.out.format(layout(padding), fill(padding, '-'));
    final var counters = result.workload.getCounters();
    for (var opcode : Opcode.values()) {
      System.out.format(layout(padding),
                        opcode,
                        String.format("%,.3f", profile[opcode.ordinal()]),
                        String.format("%,d", counters[opcode.ordinal()].get()),
                        String.format("%,.0f", 1000f * counters[opcode.ordinal()].get() / result.elapsedMs));
    }
    System.out.format(layout(padding), fill(padding, ' '));
    final var totalOps = Arrays.stream(counters).mapToLong(AtomicLong::get).sum();
    System.out.format(layout(padding),
                      "TOTAL",
                      String.format("%,.3f", 1.0),
                      String.format("%,d", totalOps),
                      String.format("%,.0f", 1000f * totalOps / result.elapsedMs));
  }

  private static void dumpSummaries(RunResult[] results, double[][] profiles) {
    final int[] padding = {15, 15, 15, 15, 15, 17, 23, 10};
    System.out.format(layout(padding), "profile", "took (s)", "ops", "rate (op/s)", "mutex failures", "snapshot failures", "antidependency failures", "efficiency");
    System.out.format(layout(padding), fill(padding, '-'));
    for (var i = 0; i < results.length; i++) {
      final var result = results[i];
      final var profile = profiles[i];
      final var counters = result.workload.getCounters();
      final var totalOps = Arrays.stream(counters).mapToLong(AtomicLong::get).sum();
      final var totalFailures = result.state.mutexFailures.get() + result.state.snapshotFailures.get() + result.state.antidependencyFailures.get();
      System.out.format(layout(padding),
                        Arrays.toString(profile),
                        String.format("%,.3f", result.elapsedMs / 1000f),
                        String.format("%,d", totalOps),
                        String.format("%,.0f", 1000f * totalOps / result.elapsedMs),
                        String.format("%,d", result.state.mutexFailures.get()),
                        String.format("%,d", result.state.snapshotFailures.get()),
                        String.format("%,d", result.state.antidependencyFailures.get()),
                        String.format("%,.3f", (double) totalOps / (totalOps + totalFailures)));
    }
  }

  private static void dumpMap(TransMap<?, Account> map) {
    for (var entry : map.dirtyView().entrySet()) {
      System.out.format("%10s:%s\n", entry.getKey(), entry.getValue());
    }
  }

  private static void checkMapSum(TransMap<?, Account> map, RunOptions options) {
    final var sum = (Long) map.dirtyView().values().stream().map(Versioned::getValue).mapToLong(Account::getBalance).sum();
    final var expectedSum = options.numAccounts * options.initialBalance;
    Assert.that(expectedSum == sum, () -> String.format("Expected: %d, actual: %d", expectedSum, sum));
  }
}
