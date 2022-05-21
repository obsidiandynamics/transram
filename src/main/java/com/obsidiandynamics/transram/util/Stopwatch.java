package com.obsidiandynamics.transram.util;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public final class Stopwatch {
  private final double cost;
  private final AtomicLong samples = new AtomicLong();
  private final AtomicLong duration = new AtomicLong();
  private long startTime;

  public Stopwatch(double cost) {
    this.cost = cost;
  }

  public void start() {
    startTime = System.nanoTime();
  }

  public void stop() {
    final var length = System.nanoTime() - startTime;
    samples.incrementAndGet();
    duration.addAndGet(length);
  }

  public long getNumSamples() {
    return samples.get();
  }

  public double getTotalDuration() {
    final var numSamples = samples.get();
    return duration.get() - cost * numSamples;
  }

  public double getMeanDuration() {
    return getTotalDuration() / samples.get();
  }

  public boolean hasSamples() {
    return samples.get() != 0;
  }

  public static final class Calibration {
    private static final long CALIBRATION_TIME_MS = 5_000;
    private static final double CALIBRATION_WARMUP_FRACTION = 0.1;

    private static volatile double cost = Double.NaN;

    public static double init() {
      if (Double.isNaN(cost)) {
        try {
          final var file = new File(".calibration");
          if (file.exists()) {
            final var lines = Files.readAllLines(file.toPath());
            Assert.that(!lines.isEmpty(), () -> String.format("Calibration file %s is empty", file));
            cost = Double.parseDouble(lines.get(0).strip());
          } else {
            System.out.format("Stopwatch calibration...\n");
            System.out.format("- Warmup...\n");
            calibrate((long) (CALIBRATION_TIME_MS * CALIBRATION_WARMUP_FRACTION));
            System.out.format("- Measurement...\n");
            cost = calibrate((long) (CALIBRATION_TIME_MS * (1.0 - CALIBRATION_WARMUP_FRACTION)));
            System.out.format("--- Invocation cost is %,.3f ns\n", cost);
            Files.write(file.toPath(), List.of(String.valueOf(cost)), StandardOpenOption.CREATE);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return cost;
    }

    public static double calibrate(long timeMs) {
      final var targetEndTime = System.nanoTime() + timeMs * 1_000_000;

      var duration = 0L;
      var calls = 0L;
      while (true) {
        calls++;
        final var startTime = System.nanoTime();
        final var elapsed = System.nanoTime() - startTime;
        if (elapsed >= 0) {
          duration += elapsed;
        }

        if (startTime > targetEndTime) {
          return (double) duration / calls;
        }
      }
    }
  }

}
