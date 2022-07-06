package com.obsidiandynamics.transram.util;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

final class HashTest {
  @Test
  void testByModulo() {
    assertThat(Hash.byModulo(0, 5)).isEqualTo(0);
    assertThat(Hash.byModulo(1, 5)).isEqualTo(1);
    assertThat(Hash.byModulo(4, 5)).isEqualTo(4);
    assertThat(Hash.byModulo(5, 5)).isEqualTo(0);
    assertThat(Hash.byModulo(6, 5)).isEqualTo(1);
    assertThat(Hash.byModulo(-1, 5)).isEqualTo(4);
    assertThat(Hash.byModulo(-4, 5)).isEqualTo(1);
    assertThat(Hash.byModulo(-5, 5)).isEqualTo(0);
    assertThat(Hash.byModulo(-6, 5)).isEqualTo(4);
  }
}