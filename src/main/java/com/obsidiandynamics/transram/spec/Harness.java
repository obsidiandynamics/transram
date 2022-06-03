package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.Map.*;
import java.util.concurrent.*;
import java.util.stream.*;

import static com.obsidiandynamics.transram.util.Table.*;

public final class Harness {
  private static final Double SCALE = Double.parseDouble(getEnvIgnoreCase("scale").orElse("1"));

  private static final long MIN_DURATION_MS = (long) (1_000 * SCALE);

  private static final double WARMUP_FRACTION = 0.1;

  private static final int INIT_OPS_PER_THREAD = 1_000;

  private static final int THREADS = Integer.parseInt(getEnvIgnoreCase("threads").orElse("16"));

  private static Optional<String> getEnvIgnoreCase(String key) {
    return System.getenv().entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase(key)).map(Entry::getValue).findAny();
  }

  private static double[] divideUnitProbs(int entries) {
    final var probs = new double[entries];
    var remaining = 1d;
    for (var i = entries - 1; i >= 0; i--) {
      probs[i] = remaining / (i + 1);
      remaining -= probs[i];
    }
    return probs;
  }

  public static <S, K, V extends DeepCloneable<V>> void run(MapFactory mapFactory, Spec<S, K, V> spec) throws InterruptedException {
    final var executor = Executors.newFixedThreadPool(THREADS);
    try {
      final var warmupMap = mapFactory.<K, V>instantiate();
      System.out.format("Running benchmarks for %s...\n", warmupMap.getClass().getSimpleName());
      System.out.format("- Warmup...\n");
      final var operationNames = spec.getOperationNames();
      final var warmupProfile = divideUnitProbs(operationNames.length);

      runOne(warmupMap, spec, warmupProfile, (long) (MIN_DURATION_MS * WARMUP_FRACTION), executor);

      final var profiles = spec.getProfiles();
      final var results = new Result[profiles.length];
      for (var i = 0; i < profiles.length; i++) {
        System.out.format("- Benchmarking profile %d of %d...\n", i + 1, profiles.length);
        final var runMap = mapFactory.<K, V>instantiate();
        final var result = runOne(runMap, spec, profiles[i], MIN_DURATION_MS, executor);
        dumpDetail(operationNames, result, profiles[i]);
        System.out.println();
        results[i] = result;
      }

      System.out.println();
      System.out.format("- Summary:\n");
      dumpProfiles(operationNames, profiles);
      System.out.println();
      dumpSummaries(results);
    } finally {
      executor.shutdown();
    }
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

    double getRate() {
      final var totalOps = Arrays.stream(dispatcher.getStopwatches()).mapToLong(Stopwatch::getNumSamples).sum();
      return 1000d * totalOps / elapsedMs;
    }
  }

  private static <S, K, V extends DeepCloneable<V>> Result runOne(TransMap<K, V> map, Spec<S, K, V> spec, double[] profile, long minDurationMs, Executor executor) throws InterruptedException {
    final var state = spec.instantiateState(map);
    final var failures = new Failures();

    final var dispatcher = new Dispatcher(profile);
    final var took = TimedRunner.<SplittableRandom>run(THREADS, INIT_OPS_PER_THREAD, minDurationMs, executor, SplittableRandom::new, rng -> {
      dispatcher.eval(rng.nextDouble(), ordinal -> spec.evaluate(ordinal, state, failures, rng));
    });

    spec.validateState(state);

    return new Result(took, dispatcher, map, failures);
  }

  private static void dumpDetail(String[] operationNames, Result result, double[] profile) {
    final var padding = new int[] {15, 10, 15, 15, 15};
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
    final var padding = new int[] {25, 15, 15, 15, 13, 15, 15, 15, 10, 10};
    System.out.format(layout(padding), "profile", "took (s)", "ops", "rate (op/s)", "mutex faults", "snapshot faults", "antidep. faults", "l.cycle faults", "efficiency", "refs");
    System.out.format(layout(padding), fill(padding, '-'));
    for (var i = 0; i < results.length; i++) {
      final var result = results[i];
      final var stopwatches = result.dispatcher.getStopwatches();
      final var totalOps = Arrays.stream(stopwatches).mapToLong(Stopwatch::getNumSamples).sum();
      final var totalFailures = result.failures.mutex.get() + result.failures.snapshot.get() + result.failures.antidependency.get();
      System.out.format(layout(padding),
                        i + 1,
                        String.format("%,.3f", result.elapsedMs / 1000f),
                        String.format("%,d", totalOps),
                        String.format("%,.0f", 1000d * totalOps / result.elapsedMs),
                        String.format("%,d", result.failures.mutex.get()),
                        String.format("%,d", result.failures.snapshot.get()),
                        String.format("%,d", result.failures.antidependency.get()),
                        String.format("%,d", result.failures.lifecycle.get()),
                        String.format("%,.3f", (double) totalOps / (totalOps + totalFailures)),
                        String.format("%,d", result.map.debug().numRefs()));
    }

    final double meanLogRate = Arrays.stream(results).map(Result::getRate).collect(Collectors.summarizingDouble(Math::log10)).getAverage();
    System.out.format("Mean log-rate: %,.4f [%,.0f]\n", meanLogRate, Math.pow(10, meanLogRate));
  }
}
