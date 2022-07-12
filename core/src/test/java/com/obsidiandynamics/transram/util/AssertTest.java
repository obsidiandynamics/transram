package com.obsidiandynamics.transram.util;

import org.junit.jupiter.api.*;

import static com.obsidiandynamics.transram.util.Assert.*;
import static org.assertj.core.api.Assertions.*;

final class AssertTest {
  @Test
  void testDefaultErrorMakerAndMessageBuilder_pass() {
    Assert.that(true);
  }

  @Test
  void testDefaultErrorMakerAndMessageBuilder_fail() {
    assertThat(catchThrowable(() -> Assert.that(false))).isInstanceOf(AssertionError.class).hasNoCause().hasMessage(null);
  }

  @Test
  void testCustomMessageBuilder_fail() {
    assertThat(catchThrowable(() -> Assert.that(false, withMessage("custom")))).isInstanceOf(AssertionError.class).hasNoCause().hasMessage("custom");
  }

  @Test
  void testCustomErrorMakerAndMessageBuilder_fail() {
    class CustomError extends AssertionError {
      CustomError(String m) { super (m, null); }
    }

    assertThat(catchThrowable(() -> Assert.that(false, CustomError::new, withMessage("custom")))).isInstanceOf(CustomError.class).hasNoCause().hasMessage("custom");
  }

  @Test
  void testIsNotNull() {
    assertThat(isNotNull("not null")).isTrue();
    assertThat(isNotNull(null)).isFalse();
  }

  @Test
  void testNot() {
    assertThat(not(false)).isTrue();
    assertThat(not(true)).isFalse();
  }
}