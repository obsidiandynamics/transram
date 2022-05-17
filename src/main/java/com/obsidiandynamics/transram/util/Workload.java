package com.obsidiandynamics.transram.util;

import java.util.*;
import java.util.concurrent.atomic.*;

public final class Workload {
  private final double[] probs;

  private final double[] cumulative;

  private final AtomicLong[] counters;

  public Workload(double[] probs) {
    Assert.that(Math.abs(Arrays.stream(probs).sum() - 1) < Double.MIN_VALUE, () -> "Ensure probabilities add to 1");
    this.probs = probs;
    cumulative = new double[probs.length - 1];
    var sum = 0d;
    for (var i = 0; i < cumulative.length; i++) {
      sum += probs[i];
      cumulative[i] = sum;
    }
    counters = new AtomicLong[probs.length];
    for (var i = 0; i < counters.length; i++) {
      counters[i] = new AtomicLong();
    }
  }

  public double[] getProbs() {
    return probs;
  }

  public AtomicLong[] getCounters() {
    return counters;
  }

  public int eval(double rnd) {
    for (var i = cumulative.length - 1; i >= 0; i--) {
      if (rnd > cumulative[i]) {
        counters[i + 1].incrementAndGet();
        return i + 1;
      }
    }
    counters[0].incrementAndGet();
    return 0;
  }

  @Override
  public String toString() {
    return Workload.class.getSimpleName() + "[probs=" + Arrays.toString(probs) + ", counters=" + Arrays.toString(counters) + ']';
  }
}
