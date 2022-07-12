package com.obsidiandynamics.transram;

import nl.jqno.equalsverifier.*;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

final class KeyRefTest {
  @Test
  void testEqualsAndHashCode() {
    EqualsVerifier.forClass(KeyRef.class).withNonnullFields("key").verify();
  }

  @Test
  void testToString() {
    final var ref = new KeyRef<String>("foo");
    final var toString = ref.toString();
    assertThat(toString).contains(KeyRef.class.getSimpleName());
    assertThat(toString).contains("foo");
  }

  @Test
  void testUnwrap() {
    assertThat(new KeyRef<String>("foo").unwrap()).isEqualTo("foo");
  }
}