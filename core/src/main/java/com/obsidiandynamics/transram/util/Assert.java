package com.obsidiandynamics.transram.util;

import java.util.function.*;

public final class Assert {
  public static void that(boolean condition) {
    that(condition, () -> "");
  }

  public static Supplier<String> withMessage(String message) {
    return () -> message;
  }

  public static void isNotNull(Object obj, Supplier<String> messageBuilder) {
    isNotNull(obj, AssertionError::new, messageBuilder);
  }

  public static void isNotNull(Object obj, Function<String, AssertionError> errorMaker, Supplier<String> messageBuilder) {
    that(obj != null, errorMaker, messageBuilder);
  }

  public static void that(boolean condition, Supplier<String> messageBuilder) {
    that(condition, AssertionError::new, messageBuilder);
  }

  public static void that(boolean condition, Function<String, AssertionError> errorMaker, Supplier<String> messageBuilder) {
    if (! condition) {
      throw errorMaker.apply(messageBuilder.get());
    }
  }
}
