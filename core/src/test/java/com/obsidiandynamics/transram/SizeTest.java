package com.obsidiandynamics.transram;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

final class SizeTest {
  @Test
  void testToString() {
    final var size = new Size(3);
    final var toString = size.toString();
    assertThat(toString).contains(Size.class.getSimpleName());
    assertThat(toString).contains("3");
  }
}