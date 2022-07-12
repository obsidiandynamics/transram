package com.obsidiandynamics.transram;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

final class RawVersionedTest {
  @Test
  void testToString() {
    final var box = StringBox.of("foo");
    final var versioned = new RawVersioned(5, box);
    final var toString = versioned.toString();
    assertThat(toString).contains(RawVersioned.class.getSimpleName());
    assertThat(toString).contains("version=5");
    assertThat(toString).contains("value=" + box);
  }
}