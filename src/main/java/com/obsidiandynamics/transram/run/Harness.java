package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.Enclose.Region.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.Map.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

import static com.obsidiandynamics.transram.util.Table.*;

public final class Harness {
  private static class RunOptions {
    int thinkTimeMs = -1;
    int numAccounts;
    int initialBalance;
    int maxXferAmount;
    int scanAccounts;
    int numThreads;
    boolean log = false;

    void validate() {
      Assert.that(thinkTimeMs >= 0);
      Assert.that(numAccounts > 0);
      Assert.that(initialBalance > 0);
      Assert.that(maxXferAmount > 0);
      Assert.that(scanAccounts > 0);
      Assert.that(numThreads > 0);
    }
  }

  private static final RunOptions RUN_OPTIONS = new RunOptions() {{
    thinkTimeMs = 0;
    numAccounts = 1_000;
    initialBalance = 100;
    maxXferAmount = 100;
    scanAccounts = 100;
    numThreads = 16;
  }};

  private static final Double SCALE = Double.parseDouble(System.getenv().entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase("SCALE")).map(Entry::getValue).findAny().orElse("1"));

  private static final long MIN_DURATION_MS = (long) (1_000 * SCALE);

  private static final double WARMUP_FRACTION = 0.1;

  //  private static final double[][] PROFILES = {
  //      {0.06, 0.04, 0.6, 0.3},
  //      {0.3, 0.2, 0.3, 0.2},
  //      {0.6, 0.3, 0.06, 0.04}
  //  };
  private static final double[][] PROFILES = {
      {0.1, 0.0, 0.6, 0.3},
      {0.5, 0.0, 0.3, 0.2},
      {0.9, 0.0, 0.06, 0.04}
  };

  private static final int INIT_OPS_PER_THREAD = 1_000;

  private static class State {
    final TransMap<Integer, Account> map;

    State(TransMap<Integer, Account> map) {
      this.map = map;
    }
  }

  private enum Opcode {
    SNAPSHOT_READ {
      @Override
      void operate(State state, Failures failures, SplittableRandom rng, RunOptions options) {
        final var firstAccountId = (int) (rng.nextDouble() * options.numAccounts);
        Enclose.over(state.map)
            .onFailure(failures::increment)
            .transact(ctx -> {
              for (var i = 0; i < options.scanAccounts; i++) {
                final var accountId = i + firstAccountId;
                ctx.read(accountId % options.numAccounts);
              }
              think(options.thinkTimeMs);
              return Action.ROLLBACK;
            });
      }
    },

    READ_ONLY {
      @Override
      void operate(State state, Failures failures, SplittableRandom rng, RunOptions options) {
        final var firstAccountId = (int) (rng.nextDouble() * options.numAccounts);
        Enclose.over(state.map)
            .onFailure(failures::increment)
            .transact(ctx -> {
              for (var i = 0; i < options.scanAccounts; i++) {
                final var accountId = i + firstAccountId;
                ctx.read(accountId % options.numAccounts);
              }
              think(options.thinkTimeMs);
              return Action.COMMIT;
            });
      }
    },

    XFER {
      @Override
      void operate(State state, Failures failures, SplittableRandom rng, RunOptions options) {
        Enclose.over(state.map)
            .onFailure(failures::increment)
            .transact(ctx -> {
              final var fromAccountId = (int) (rng.nextDouble() * options.numAccounts);
              final var toAccountId = (int) (rng.nextDouble() * options.numAccounts);
              final var amount = 1 + (int) (rng.nextDouble() * (options.maxXferAmount - 1));
              if (toAccountId == fromAccountId) {
                return Action.ROLLBACK_AND_RESET;
              }
              if (options.log)
                System.out.format("%s, fromAccountId=%d, toAccountId=%d, amount=%d\n", Thread.currentThread().getName(), fromAccountId, toAccountId, amount);
              final var fromAccount = ctx.read(fromAccountId);
              if (fromAccount == null) {
                return Action.ROLLBACK_AND_RESET;
              }
              final var toAccount = ctx.read(toAccountId);
              if (toAccount == null) {
                return Action.ROLLBACK_AND_RESET;
              }

              final var newFromBalance = fromAccount.getBalance() - amount;
              if (newFromBalance < 0) {
                return Action.ROLLBACK_AND_RESET;
              }

              fromAccount.setBalance(newFromBalance);
              toAccount.setBalance(toAccount.getBalance() + amount);

              ctx.write(fromAccountId, fromAccount);
              ctx.write(toAccountId, toAccount);
              think(options.thinkTimeMs);
              return Action.COMMIT;
            });
      }
    },

    SPLIT_MERGE {
      @Override
      void operate(State state, Failures failures, SplittableRandom rng, RunOptions options) {
        Enclose.over(state.map)
            .onFailure(failures::increment)
            .transact(ctx -> {
              final var accountAId = (int) (rng.nextDouble() * options.numAccounts);
              final var accountBId = (int) (rng.nextDouble() * options.numAccounts);
              if (accountAId == accountBId) {
                return Action.ROLLBACK_AND_RESET;
              }
              final var accountA = ctx.read(accountAId);
              final var accountB = ctx.read(accountBId);
              if (accountA == null && accountB == null) {
                return Action.ROLLBACK_AND_RESET;
              } else if (accountA == null) {
                if (accountB.getBalance() == 0) {
                  return Action.ROLLBACK_AND_RESET;
                }
                final var xferAmount = 1 + (int) (rng.nextDouble() * (accountB.getBalance() - 1));
                final var newAccountA = new Account(accountAId, xferAmount);
                ctx.write(accountAId, newAccountA);
                accountB.setBalance(accountB.getBalance() - xferAmount);
                ctx.write(accountBId, accountB);
              } else if (accountB == null) {
                if (accountA.getBalance() == 0) {
                  return Action.ROLLBACK_AND_RESET;
                }
                final var xferAmount = 1 + (int) (rng.nextDouble() * (accountA.getBalance() - 1));
                final var newAccountB = new Account(accountBId, xferAmount);
                ctx.write(accountBId, newAccountB);
                accountA.setBalance(accountA.getBalance() - xferAmount);
                ctx.write(accountAId, accountA);
              } else if (rng.nextDouble() > 0.5) {
                accountA.setBalance(accountA.getBalance() + accountB.getBalance());
                ctx.write(accountAId, accountA);
                ctx.write(accountBId, null);
              } else {
                accountB.setBalance(accountA.getBalance() + accountB.getBalance());
                ctx.write(accountBId, accountB);
                ctx.write(accountAId, null);
              }
              return Action.COMMIT;
            });
      }
    };

    abstract void operate(State state, Failures failures, SplittableRandom rng, RunOptions options);

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
    System.out.format("Running benchmarks for %s...\n", mapFactory.get().getClass().getSimpleName());
    System.out.format("- Warmup...\n");
    final var warmupProfile = new double[]{0.33, 0.33, 0.34};
    runOne(mapFactory, RUN_OPTIONS, warmupProfile, (long) (MIN_DURATION_MS * WARMUP_FRACTION));

    final var results = new Result[PROFILES.length];
    for (var i = 0; i < PROFILES.length; i++) {
      System.out.format("- Benchmarking profile %d of %d...\n", i + 1, PROFILES.length);
      final var result = runOne(mapFactory, RUN_OPTIONS, PROFILES[i], MIN_DURATION_MS);
      dumpDetail(result, PROFILES[i]);
      System.out.println();
      results[i] = result;
    }

    System.out.println();
    System.out.format("- Summary:\n");
    dumpProfiles(Opcode.values(), PROFILES);
    System.out.println();
    dumpSummaries(results);
  }

  private static class Result {
    final long elapsedMs;
    final Dispatcher dispatcher;
    final State state;
    final Failures failures;

    Result(long elapsedMs, Dispatcher dispatcher, State state, Failures failures) {
      this.elapsedMs = elapsedMs;
      this.dispatcher = dispatcher;
      this.state = state;
      this.failures = failures;
    }
  }

  private static Result runOne(Supplier<TransMap<Integer, Account>> mapFactory, RunOptions options, double[] profile, long minDurationMs) throws InterruptedException {
    final var map = mapFactory.get();
    final var state = new State(map);
    final var failures = new Failures();

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

    final var dispatcher = new Dispatcher(profile);
    final var latch = new CountDownLatch(options.numThreads);
    final var startTime = System.currentTimeMillis();
    for (var i = 0; i < options.numThreads; i++) {
      new Thread(() -> {
        final var rng = new SplittableRandom();
        var opsPerThread = INIT_OPS_PER_THREAD;
        var op = 0;
        while (true) {
          dispatcher.eval(rng.nextDouble(), ordinal -> Opcode.values()[ordinal].operate(state, failures, rng, options));
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

    return new Result(took, dispatcher, state, failures);
  }

  private static void dumpDetail(Result result, double[] profile) {
    final int[] padding = {15, 10, 15, 15, 15};
    System.out.format(layout(padding), "operation", "p(op)", "ops", "rate (op/s)", "ns/op");
    System.out.format(layout(padding), fill(padding, '-'));
    final var stopwatches = result.dispatcher.getStopwatches();
    final var totalOps = Arrays.stream(stopwatches).mapToLong(Stopwatch::getNumSamples).sum();
    for (var opcode : Opcode.values()) {
      final var stopwatch = stopwatches[opcode.ordinal()];
      System.out.format(layout(padding),
                        opcode,
                        String.format("%,.3f", profile[opcode.ordinal()]),
                        String.format("%,d", stopwatch.getNumSamples()),
                        stopwatch.hasSamples() ? String.format("%,.0f", 1_000_000_000 / stopwatch.getMeanDuration()) : '-',
                        stopwatch.hasSamples() ? String.format("%,.0f", stopwatch.getMeanDuration()) : '-');
    }

    System.out.format(layout(padding), fill(padding, ' '));
    System.out.format(layout(padding),
                      "TOTAL",
                      String.format("%,.3f", 1.0),
                      String.format("%,d", totalOps),
                      String.format("%,.0f", 1000f * totalOps / result.elapsedMs),
                      String.format("%,.0f", result.elapsedMs * 1_000_000f / totalOps));
  }

  private static void dumpProfiles(Opcode[] opcodes, double[][] profiles) {
    final var padding = new int[profiles.length + 1];
    padding[0] = 25;
    for (var i = 1; i < padding.length; i++) {
      padding[i] = 5;
    }
    final var cols = new ArrayList<String>(padding.length);
    cols.add("operation\\profile");
    cols.addAll(IntStream.range(1, profiles.length + 1).boxed().map(String::valueOf).collect(Collectors.toList()));
    System.out.format(layout(padding), cols.toArray());
    System.out.format(layout(padding), fill(padding, '-'));
    for (var opcodeOrdinal = 0; opcodeOrdinal < opcodes.length; opcodeOrdinal++) {
      cols.set(0, opcodes[opcodeOrdinal].name());
      for (var profileIdx = 0; profileIdx < profiles.length; profileIdx++) {
        cols.set(profileIdx + 1, String.valueOf(profiles[profileIdx][opcodeOrdinal]));
      }
      System.out.format(layout(padding), cols.toArray());
    }
  }

  private static void dumpSummaries(Result[] results) {
    final int[] padding = {25, 15, 15, 15, 13, 15, 15, 10, 10};
    System.out.format(layout(padding), "profile", "took (s)", "ops", "rate (op/s)", "mutex faults", "snapshot faults", "antidep. faults", "efficiency", "refs");
    System.out.format(layout(padding), fill(padding, '-'));
    for (var i = 0; i < results.length; i++) {
      final var result = results[i];
      final var counters = result.dispatcher.getStopwatches();
      final var totalOps = Arrays.stream(counters).mapToLong(Stopwatch::getNumSamples).sum();
      final var totalFailures = result.failures.mutex.get() + result.failures.snapshot.get() + result.failures.antidependency.get();
      System.out.format(layout(padding),
                        i + 1,
                        String.format("%,.3f", result.elapsedMs / 1000f),
                        String.format("%,d", totalOps),
                        String.format("%,.0f", 1000f * totalOps / result.elapsedMs),
                        String.format("%,d", result.failures.mutex.get()),
                        String.format("%,d", result.failures.snapshot.get()),
                        String.format("%,d", result.failures.antidependency.get()),
                        String.format("%,.3f", (double) totalOps / (totalOps + totalFailures)),
                        String.format("%,d", result.state.map.debug().numRefs()));
    }
  }

  private static void dumpMap(TransMap<?, Account> map) {
    for (var entry : map.debug().dirtyView().entrySet()) {
      System.out.format("%10s:%s\n", entry.getKey(), entry.getValue());
    }
  }

  private static void checkMapSum(TransMap<?, Account> map, RunOptions options) {
    final var values = map.debug().dirtyView().values();
    final var sum = values.stream().filter(Versioned::hasValue).map(Versioned::getValue).mapToLong(Account::getBalance).sum();
    final var expectedSum = options.numAccounts * options.initialBalance;
    Assert.that(expectedSum == sum, () -> String.format("Expected: %d, actual: %d", expectedSum, sum));
    final var min = values.stream().filter(Versioned::hasValue).map(Versioned::getValue).mapToLong(Account::getBalance).min().orElseThrow();
    Assert.that(min >= 0, () -> String.format("Minimum balance is %d", min));
  }
}
