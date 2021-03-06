package com.obsidiandynamics.transram.util;

import java.util.function.*;

public final class Assert {
  private Assert() {}

  public static void that(boolean condition) {
    that(condition, () -> null);
  }

  public static Supplier<String> withMessage(String message) {
    return () -> message;
  }

  public static void that(boolean condition, Supplier<String> messageBuilder) {
    that(condition, m -> new AssertionError(m, null), messageBuilder);
  }

  public static void that(boolean condition, Function<String, AssertionError> errorMaker, Supplier<String> messageBuilder) {
    if (! condition) {
      throw errorMaker.apply(messageBuilder.get());
    }
  }

  public static boolean isNotNull(Object obj) {
    return obj != null;
  }

  public static boolean not(boolean b) {
    return !b;
  }
}
