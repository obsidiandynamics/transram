package com.obsidiandynamics.transram.util;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

final class UnsafeTest {
  @Test
  void testCastNull() {
    assertThat(Unsafe.<Void>cast(null)).isNull();
  }

  @Test
  void testCastNonNull() {
    assertThat(Unsafe.<String>cast("foo")).isEqualTo("foo");
  }

  @Test
  void testCastException() {
    assertThat(catchThrowable(() -> {
      var s = Unsafe.<String>cast(1);
      throw new AssertionError("shouldn't reach here");
    })).isInstanceOf(ClassCastException.class);
  }
}