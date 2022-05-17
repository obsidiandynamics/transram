package com.obsidiandynamics.transram.util;

import java.util.function.*;

public final class Assert {
  public static void that(boolean condition) {
    that(condition, () -> "");
  }

  public static void that(boolean condition, Supplier<String> messageBuilder) {
    if (! condition) {
      throw new AssertionError(messageBuilder.get());
    }
  }
}
