package com.obsidiandynamics.transram.util;

public final class Unsafe {
  private Unsafe() {}

  @SuppressWarnings("unchecked")
  public static <T> T cast(Object obj) {
    return (T) obj;
  }
}
