package com.obsidiandynamics.transram;

import nl.jqno.equalsverifier.*;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

final class GenericVersionedTest {
  @Test
  void testEqualsAndHashCode() {
    EqualsVerifier.forClass(GenericVersioned.class).verify();
  }

  @Test
  void testToString() {
    final var box = StringBox.of("foo");
    final var versioned = new GenericVersioned<StringBox>(5, box);
    final var toString = versioned.toString();
    assertThat(toString).contains(GenericVersioned.class.getSimpleName());
    assertThat(toString).contains("version=5");
    assertThat(toString).contains("value=" + String.valueOf(box));
  }

  @Test
  void testDeepClone_withNullValue() {
    final var original = new GenericVersioned<StringBox>(5, null);
    assertThat(original.deepClone()).isSameAs(original);
  }

  @Test
  void testDeepClone_withNonNullValue() {
    final var value = StringBox.of("foo");
    final var original = new GenericVersioned<StringBox>(5, value);
    final var clone = original.deepClone();
    assertThat(clone).isNotSameAs(original);
    assertThat(clone).isEqualTo(original);
    assertThat(clone.getVersion()).isEqualTo(5);
    assertThat(clone.getValue()).isNotSameAs(value);
    assertThat(clone.getValue()).isEqualTo(value);
  }

  @Test
  void testHasValue() {
    assertThat(new GenericVersioned<StringBox>(5, null).hasValue()).isFalse();
    assertThat(new GenericVersioned<StringBox>(5, StringBox.of("foo")).hasValue()).isTrue();
  }
}