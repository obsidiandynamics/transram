package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.SrmlMap.*;
import com.obsidiandynamics.transram.TransContext.*;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

class SrmlContextTest {
  private static <K, V extends DeepCloneable<V>> TransMap<K, V> newMap() {
    return new SrmlMap<>(new Options());
  }

  private static <K, V extends DeepCloneable<V>> TransMap<K, V> newMap(Class<K> keyClass, Class<V> valueClass) {
    return SrmlContextTest.newMap();
  }

  @Nested
  class BasicTests {
    @Test
    void testEmptyMapAppearsEmpty() throws ConcurrentModeFailure {
      final var map = newMap();
      final var ctx = map.transact();
      assertThat(ctx.size()).isEqualTo(0);
      assertThat(ctx.keys(__ -> true)).isEmpty();
    }

    @Test
    void testStateBeforeAndAfterCommit() throws ConcurrentModeFailure {
      final var map = newMap();
      assertThat(map.debug().getVersion()).isEqualTo(0);
      final var ctx = map.transact();
      assertThat(ctx.getState()).isEqualTo(State.OPEN);
      ctx.commit();
      assertThat(ctx.getState()).isEqualTo(State.COMMITTED);
      assertThat(ctx.getVersion()).isEqualTo(1);
      assertThat(map.debug().getVersion()).isEqualTo(1);
    }

    @Test
    void testStateAfterRollback() throws ConcurrentModeFailure {
      final var map = newMap();
      assertThat(map.debug().getVersion()).isEqualTo(0);
      final var ctx = map.transact();
      assertThat(ctx.getState()).isEqualTo(State.OPEN);
      ctx.rollback();
      assertThat(ctx.getState()).isEqualTo(State.ROLLED_BACK);
      assertThat(catchThrowable(ctx::getVersion)).isInstanceOf(IllegalStateException.class);
      assertThat(map.debug().getVersion()).isEqualTo(0);
    }

    @Test
    void testCommitOrRollbackAfterCommit() throws ConcurrentModeFailure {
      final var map = newMap();
      final var ctx = map.transact();
      ctx.commit();
      assertThat(catchThrowable(ctx::commit)).isInstanceOf(IllegalStateException.class);
      assertThat(catchThrowable(ctx::rollback)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCommitOrRollbackAfterRollback() throws ConcurrentModeFailure {
      final var map = newMap();
      final var ctx = map.transact();
      ctx.rollback();
      assertThat(catchThrowable(ctx::commit)).isInstanceOf(IllegalStateException.class);
      assertThat(catchThrowable(ctx::rollback)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void noChangeAfterRollback() throws ConcurrentModeFailure {
      final var map = newMap(Integer.class, StringBox.class);
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.insert(1, StringBox.of("one_v0"));
        ctx.commit();
        assertThat(map.debug().getVersion()).isEqualTo(1);
      }
      {
        final var ctx = map.transact();
        ctx.update(0, StringBox.of("zero_v1"));
        assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v1"));
        ctx.delete(1);
        assertThat(ctx.read(1)).isNull();
        ctx.insert(2, StringBox.of("two_v0"));
        assertThat(ctx.read(2)).isEqualTo(StringBox.of("two_v0"));
        ctx.insert(3, StringBox.of("three_v0"));
        assertThat(ctx.read(3)).isEqualTo(StringBox.of("three_v0"));
        assertThat(ctx.size()).isEqualTo(3);
        ctx.rollback();
        assertThat(map.debug().getVersion()).isEqualTo(1);
      }
      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v0"));
        assertThat(ctx.read(1)).isEqualTo(StringBox.of("one_v0"));
        assertThat(ctx.read(2)).isNull();
        assertThat(ctx.read(3)).isNull();
        assertThat(ctx.size()).isEqualTo(2);
        assertThat(ctx.keys(__ -> true)).containsExactly(0, 1);
      }
    }
  }

  @Nested
  class LifecycleTests {
    @Test
    void testInsertThenUpdateThenDeleteOfNonExistent() throws ConcurrentModeFailure {
      final var map = newMap(Integer.class, StringBox.class);
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v1"));
        final var valueAfterInsert = ctx.read(0);
        assertThat(valueAfterInsert).isEqualTo(StringBox.of("zero_v1"));
        assertThat(ctx.size()).isEqualTo(1);

        valueAfterInsert.setValue("zero_v2");
        ctx.update(0, valueAfterInsert);
        final var valueAfterUpdate = ctx.read(0);
        assertThat(valueAfterUpdate).isEqualTo(StringBox.of("zero_v2"));
        assertThat(ctx.size()).isEqualTo(1);

        ctx.delete(0);
        final var valueAfterDelete = ctx.read(0);
        assertThat(valueAfterDelete).isNull();
        assertThat(ctx.size()).isEqualTo(0);
        ctx.commit();
        assertThat(ctx.getVersion()).isEqualTo(1);
      }
      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isNull();
        assertThat(ctx.size()).isEqualTo(0);
      }
    }

    @Test
    void testDeleteThenInsertOfExisting() throws ConcurrentModeFailure {
      final var map = newMap(Integer.class, StringBox.class);
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v1"));
        ctx.commit();
        assertThat(ctx.getVersion()).isEqualTo(1);
      }
      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v1"));
        assertThat(ctx.size()).isEqualTo(1);

        ctx.delete(0);
        assertThat(ctx.read(0)).isNull();
        assertThat(ctx.size()).isEqualTo(0);

        ctx.insert(0, StringBox.of("zero_v3"));
        assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v3"));
        assertThat(ctx.size()).isEqualTo(1);
        ctx.commit();
        assertThat(ctx.getVersion()).isEqualTo(2);
      }
      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v3"));
        assertThat(ctx.size()).isEqualTo(1);
      }
    }

    @Test
    void testInsertOfExisting() {
      final var map = newMap(Integer.class, StringBox.class);
    }
  }


}
