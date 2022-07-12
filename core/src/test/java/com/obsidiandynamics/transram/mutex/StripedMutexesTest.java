package com.obsidiandynamics.transram.mutex;

import nl.jqno.equalsverifier.*;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

final class StripedMutexesTest {
  @Test
  void testToString() {
    final var mutexes = new StripedMutexes<UpgradeableMutex>(2, UnfairUpgradeableMutex::new);
    final var toString = mutexes.toString();
    assertThat(toString).contains(StripedMutexes.class.getSimpleName());
    assertThat(toString).contains("stripes.length=2");
  }

  @Nested
  final class MutexRefTests {
    @Test
    void testEqualsAndHashCode() {
      EqualsVerifier.forClass(StripedMutexes.MutexRef.class)
          .withIgnoredFields("mutex")
          .verify();
    }

    @Test
    void testToString() {
      final var mutex = new UnfairUpgradeableMutex();
      final var ref = new StripedMutexes.MutexRef<UpgradeableMutex>(7, mutex);
      final var toString = ref.toString();
      assertThat(toString).contains(StripedMutexes.MutexRef.class.getSimpleName());
      assertThat(toString).contains("stripe=7");
      assertThat(toString).contains("mutex=" + mutex);
    }
  }
}