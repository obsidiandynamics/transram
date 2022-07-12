package com.obsidiandynamics.transram.util;

import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.*;

public final class CasTest {
  @Test
  void testCasSuccessfulOnFirstAttempt() {
    final var ctr = new AtomicLong(5);
    assertThat(Cas.compareAndSetConditionally(ctr, 6, Cas.lowerThan(6))).isEqualTo(5L);
    assertThat(ctr.get()).isEqualTo(6L);
  }

  @Test
  void testCasSuccessfulOnRepeatAttempt() {
    final var ctr = Mockito.spy(new AtomicLong());
    final var attempts = new AtomicInteger();
    Mockito.doAnswer(__ -> attempts.incrementAndGet() == 1 ? 4L : 5L).when(ctr).get();
    Mockito.doReturn(false).when(ctr).compareAndSet(Mockito.eq(4L), Mockito.eq(6L));
    Mockito.doReturn(true).when(ctr).compareAndSet(Mockito.eq(5L), Mockito.eq(6L));
    assertThat(Cas.compareAndSetConditionally(ctr, 6, Cas.lowerThan(6))).isEqualTo(5L);
  }

  @Test
  void testCasPredicateFailure() {
    final var ctr = new AtomicLong(5);
    assertThat(Cas.compareAndSetConditionally(ctr, 4, Cas.lowerThan(5))).isEqualTo(5L);
    assertThat(ctr.get()).isEqualTo(5L);
  }
}
