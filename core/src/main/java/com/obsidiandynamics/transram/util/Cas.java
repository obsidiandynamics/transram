package com.obsidiandynamics.transram.util;

import java.util.concurrent.atomic.*;
import java.util.function.*;

public final class Cas {
  private Cas() {}

  public static long compareAndSetConditionally(AtomicLong atomicLong, long newValue, LongPredicate condition) {
    while (true) {
      final var existingValue = atomicLong.get();
      if (condition.test(existingValue)) {
        final var updated = atomicLong.compareAndSet(existingValue, newValue);
        if (updated) {
          return existingValue;
        }
      } else {
        return existingValue;
      }
    }
  }

  public static LongPredicate lowerThan(long comparand) {
    return value -> value < comparand;
  }
}
