package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.run.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.Map.*;
import java.util.concurrent.*;
import java.util.stream.*;

import static com.obsidiandynamics.transram.util.Table.*;

public final class SpecHarness {
  private static final Double SCALE = Double.parseDouble(System.getenv().entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase("SCALE")).map(Entry::getValue).findAny().orElse("1"));

  private static final long MIN_DURATION_MS = (long) (5_000 * SCALE);

  private static final double WARMUP_FRACTION = 0.1;

  private static final int INIT_OPS_PER_THREAD = 1_000;

  private static final int THREADS = 16;

  public static <S, K, V extends DeepCloneable<V>> void run(MapFactory mapFactory, Spec<S, K, V> spec) throws InterruptedException {
    final var warmupMap = mapFactory.<K, V>instantiate();
    System.out.format("Running benchmarks for %s...\n", warmupMap.getClass().getSimpleName());
    System.out.format("- Warmup...\n");
    final var warmupProfile = new double[]{0.33, 0.33, 0.34};
    runOne(warmupMap, spec, warmupProfile, (long) (MIN_DURATION_MS * WARMUP_FRACTION));

    final var operationNames = spec.getOperationNames();
    final var profiles = spec.getProfiles();
    final var results = new Result[profiles.length];
    for (var i = 0; i < profiles.length; i++) {
      System.out.format("- Benchmarking profile %d of %d...\n", i + 1, profiles.length);
      final var runMap = mapFactory.<K, V>instantiate();
      final var result = runOne(runMap, spec, profiles[i], MIN_DURATION_MS);
      dumpDetail(operationNames, result, profiles[i]);
      System.out.println();
      results[i] = result;
    }

    System.out.println();
    System.out.format("- Summary:\n");
    dumpProfiles(operationNames, profiles);
    System.out.println();
    dumpSummaries(results);
  }

  private static class Result {
    final long elapsedMs;
    final Dispatcher dispatcher;
    final TransMap<?, ?> map;
    final Failures failures;

    Result(long elapsedMs, Dispatcher dispatcher, TransMap<?, ?> map, Failures failures) {
      this.elapsedMs = elapsedMs;
      this.dispatcher = dispatcher;
      this.map = map;
      this.failures = failures;
    }
  }

  private static <S, K, V extends DeepCloneable<V>> Result runOne(TransMap<K, V> map, Spec<S, K, V> spec, double[] profile, long minDurationMs) throws InterruptedException {
    final var state = spec.instantiateState(map);
    final var failures = new Failures();

    final var dispatcher = new Dispatcher(profile);
    final var latch = new CountDownLatch(THREADS);
    final var startTime = System.currentTimeMillis();
    for (var i = 0; i < THREADS; i++) {
      new Thread(() -> {
        final var rng = new SplittableRandom();
        var opsPerThread = INIT_OPS_PER_THREAD;
        var op = 0;
        while (true) {
          dispatcher.eval(rng.nextDouble(), ordinal -> spec.evaluate(ordinal, state, failures, rng));
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

    spec.validateState(state);

    return new Result(took, dispatcher, map, failures);
  }

  private static void dumpDetail(String[] operationNames, Result result, double[] profile) {
    final int[] padding = {15, 10, 15, 15, 15};
    System.out.format(layout(padding), "operation", "p(op)", "ops", "rate (op/s)", "ns/op");
    System.out.format(layout(padding), fill(padding, '-'));
    final var stopwatches = result.dispatcher.getStopwatches();
    final var totalOps = Arrays.stream(stopwatches).mapToLong(Stopwatch::getNumSamples).sum();
    for (var ordinal = 0; ordinal < operationNames.length; ordinal++) {
      final var opcode = operationNames[ordinal];
      final var stopwatch = stopwatches[ordinal];
      System.out.format(layout(padding),
                        opcode,
                        String.format("%,.3f", profile[ordinal]),
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

  private static void dumpProfiles(String[] operationNames, double[][] profiles) {
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
    for (var ordinal = 0; ordinal < operationNames.length; ordinal++) {
      cols.set(0, operationNames[ordinal]);
      for (var profileIdx = 0; profileIdx < profiles.length; profileIdx++) {
        cols.set(profileIdx + 1, String.valueOf(profiles[profileIdx][ordinal]));
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
                        String.format("%,d", result.map.debug().numRefs()));
    }
  }
}
