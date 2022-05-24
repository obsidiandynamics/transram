package com.obsidiandynamics.transram.util;

import com.obsidiandynamics.transram.util.Stopwatch.*;

import java.util.*;
import java.util.function.*;

public final class Dispatcher {
  private final double[] probs;

  private final double[] cumulative;

  private final Stopwatch[] stopwatches;

  public Dispatcher(double[] probs) {
    Assert.that(Math.abs(Arrays.stream(probs).sum() - 1) < Double.MIN_VALUE, () -> "Ensure probabilities sum to 1");
    this.probs = probs;
    cumulative = new double[probs.length - 1];
    var sum = 0d;
    for (var i = 0; i < cumulative.length; i++) {
      sum += probs[i];
      cumulative[i] = sum;
    }
    stopwatches = new Stopwatch[probs.length];
    for (var i = 0; i < stopwatches.length; i++) {
      stopwatches[i] = new Stopwatch(Calibration.init());
    }
  }

  public double[] getProbs() {
    return probs;
  }

  public Stopwatch[] getStopwatches() {
    return stopwatches;
  }

  public int eval(double rnd, IntConsumer runner) {
    var selected = 0;
    for (var i = cumulative.length - 1; i >= 0; i--) {
      if (rnd > cumulative[i]) {
        selected = i + 1;
        break;
      }
    }
    final var stopwatch = stopwatches[selected];
    stopwatch.start();
    runner.accept(selected);
    stopwatch.stop();
    return selected;
  }

  @Override
  public String toString() {
    return Dispatcher.class.getSimpleName() + "[probs=" + Arrays.toString(probs) + ", counters=" + Arrays.toString(stopwatches) + ']';
  }
}
