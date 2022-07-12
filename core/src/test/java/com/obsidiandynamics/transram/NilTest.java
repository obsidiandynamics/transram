package com.obsidiandynamics.transram;

import org.junit.jupiter.api.*;

import java.lang.reflect.*;

import static org.assertj.core.api.Assertions.*;

final class NilTest {
  @Test
  void testSingleton() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
    assertThat(Nil.instance()).isSameAs(Nil.instance());
    assertThat(Nil.instance().hashCode()).isEqualTo(Nil.instance().hashCode());
    assertThat(Nil.instance()).isEqualTo(Nil.instance());
    assertThat(Nil.instance().deepClone()).isEqualTo(Nil.instance());

    final var nilConstructor = Nil.class.getDeclaredConstructor();
    nilConstructor.setAccessible(true);
    final var anotherNilInstance = nilConstructor.newInstance();
    assertThat(Nil.instance()).isNotEqualTo(anotherNilInstance);
  }

  @Test
  void testToString() {
    assertThat(Nil.instance().toString()).contains(Nil.class.getSimpleName());
  }
}