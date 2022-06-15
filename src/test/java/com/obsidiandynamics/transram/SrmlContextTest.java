package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.LifecycleFailure.*;
import com.obsidiandynamics.transram.SrmlMap.*;
import com.obsidiandynamics.transram.TransContext.*;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

class SrmlContextTest {
  private static <K, V extends DeepCloneable<V>> TransMap<K, V> newMap() {
    return new SrmlMap<>(new Options());
  }

  @Nested
  class BasicTests {
    @Test
    void testEmptyMapAppearsEmpty() throws ConcurrentModeFailure {
      final var map = newMap();
      final var ctx = map.transact();
      assertThat(ctx.size()).isEqualTo(0);
      assertThat(ctx.keys(__ -> true)).isEmpty();
      assertThat(ctx.read(0)).isNull();
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
      assertThat(catchThrowable(ctx::getVersion)).isExactlyInstanceOf(TransactionNotCommittedException.class);
      assertThat(map.debug().getVersion()).isEqualTo(0);
    }

    @Test
    void testOperationsAfterCommit() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      final var ctx = map.transact();
      ctx.commit();
      assertThat(catchThrowable(ctx::commit)).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(ctx::rollback)).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(() -> ctx.read(0))).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(() -> ctx.insert(0, Nil.instance()))).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(() -> ctx.update(0, Nil.instance()))).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(() -> ctx.delete(0))).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(ctx::size)).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(() -> ctx.keys(__ -> true))).isExactlyInstanceOf(TransactionNotOpenException.class);
    }

    @Test
    void testCommitOrRollbackAfterRollback() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      final var ctx = map.transact();
      ctx.rollback();
      assertThat(catchThrowable(ctx::commit)).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(ctx::rollback)).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(() -> ctx.read(0))).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(() -> ctx.insert(0, Nil.instance()))).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(() -> ctx.update(0, Nil.instance()))).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(() -> ctx.delete(0))).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(ctx::size)).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(() -> ctx.keys(__ -> true))).isExactlyInstanceOf(TransactionNotOpenException.class);
      assertThat(catchThrowable(ctx::getVersion)).isExactlyInstanceOf(TransactionNotCommittedException.class);
    }

    @Test
    void noChangeAfterRollback() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, StringBox>newMap();
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
        assertThat(ctx.keys(__ -> true)).containsExactly(0, 2, 3);
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

    @Test
    void testNullKeyOrValue() {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      final var ctx = map.transact();
      assertThat(catchThrowable(() -> ctx.read(null))).isExactlyInstanceOf(NullKeyAssertionError.class);
      assertThat(catchThrowable(() -> ctx.insert(null, Nil.instance()))).isExactlyInstanceOf(NullKeyAssertionError.class);
      assertThat(catchThrowable(() -> ctx.insert(0, null))).isExactlyInstanceOf(NullValueAssertionError.class);
      assertThat(catchThrowable(() -> ctx.update(null, Nil.instance()))).isExactlyInstanceOf(NullKeyAssertionError.class);
      assertThat(catchThrowable(() -> ctx.update(0, null))).isExactlyInstanceOf(NullValueAssertionError.class);
      assertThat(catchThrowable(() -> ctx.delete(null))).isExactlyInstanceOf(NullKeyAssertionError.class);
    }

    @Test
    void testReadThenUpdateNonexistent() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      final var ctx = map.transact();
      assertThat(ctx.read(0)).isNull();
      assertThat(catchThrowableOfType(() -> ctx.update(0, Nil.instance()), IllegalLifecycleStateException.class)
                     .getReason()).isEqualTo(IllegalLifecycleStateException.Reason.UPDATE_NONEXISTENT);
    }

    @Test
    void testReadThenDeleteNonexistent() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      final var ctx = map.transact();
      assertThat(ctx.read(0)).isNull();
      assertThat(catchThrowableOfType(() -> ctx.delete(0), IllegalLifecycleStateException.class)
                     .getReason()).isEqualTo(IllegalLifecycleStateException.Reason.DELETE_NONEXISTENT);
    }

    @Test
    void testReadThenInsertExisting() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, Nil.instance());
        ctx.commit();
      }
      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isEqualTo(Nil.instance());
        assertThat(catchThrowableOfType(() -> ctx.insert(0, Nil.instance()), IllegalLifecycleStateException.class)
                       .getReason()).isEqualTo(IllegalLifecycleStateException.Reason.INSERT_EXISTING);
      }
    }

    @Test
    void testInsertAfterInsert() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      final var ctx = map.transact();
      ctx.insert(0, Nil.instance());
      assertThat(ctx.size()).isEqualTo(1);
      assertThat(catchThrowableOfType(() -> ctx.insert(0, Nil.instance()), IllegalLifecycleStateException.class)
                     .getReason()).isEqualTo(IllegalLifecycleStateException.Reason.INSERT_EXISTING);
    }

    @Test
    void testNegativeSize() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      final var ctx = map.transact();
      assertThat(catchThrowableOfType(() -> ctx.delete(0), IllegalLifecycleStateException.class)
                     .getReason()).isEqualTo(IllegalLifecycleStateException.Reason.NEGATIVE_SIZE);
    }

    @Test
    void testUpdateAfterDelete() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      final var ctx = map.transact();
      ctx.insert(0, Nil.instance());
      ctx.delete(0);
      assertThat(ctx.size()).isEqualTo(0); // no change after insert-delete
      assertThat(catchThrowableOfType(() -> ctx.update(0, Nil.instance()), IllegalLifecycleStateException.class)
                     .getReason()).isEqualTo(IllegalLifecycleStateException.Reason.UPDATE_NONEXISTENT);
    }

    @Test
    void testDeleteAfterDelete() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      final var ctx = map.transact();
      ctx.insert(0, Nil.instance());
      ctx.delete(0);
      assertThat(ctx.size()).isEqualTo(0); // no change after insert-delete
      assertThat(catchThrowableOfType(() -> ctx.delete(0), IllegalLifecycleStateException.class)
                     .getReason()).isEqualTo(IllegalLifecycleStateException.Reason.DELETE_NONEXISTENT);
    }
  }

  @Nested
  class LifecycleTests {
    @Test
    void testInsertThenUpdateThenDeleteOfNonexistent() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, StringBox>newMap();
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
      final var map = SrmlContextTest.<Integer, StringBox>newMap();
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
    void testInsertOfExisting() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, Nil.instance());
        ctx.commit();
      }
      {
        final var ctx = map.transact();
        ctx.insert(0, Nil.instance());
        assertThat(catchThrowableOfType(ctx::commit, LifecycleFailure.class)
                       .getReason()).isEqualTo(Reason.INSERT_EXISTING);
        assertThat(ctx.getState()).isEqualTo(State.ROLLED_BACK);
      }
    }

    @Test
    void testUpdateOfNonexistent() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      final var ctx = map.transact();
      ctx.update(0, Nil.instance());
      assertThat(catchThrowableOfType(ctx::commit, LifecycleFailure.class)
                     .getReason()).isEqualTo(Reason.UPDATE_NONEXISTENT);
      assertThat(ctx.getState()).isEqualTo(State.ROLLED_BACK);
    }

    @Test
    void testDeleteOfNonexistent() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      final var ctx = map.transact();
      ctx.insert(0, Nil.instance()); // inserting an item here prevents negative size upon later delete
      ctx.delete(1);
      assertThat(catchThrowableOfType(ctx::commit, LifecycleFailure.class)
                     .getReason()).isEqualTo(Reason.DELETE_NONEXISTENT);
      assertThat(ctx.getState()).isEqualTo(State.ROLLED_BACK);
    }

    @Test
    void testInsertDeleteOfNonexistent() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, Nil>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, Nil.instance());
        ctx.commit();
      }
      {
        final var ctx = map.transact();
        ctx.insert(0, Nil.instance());
        ctx.delete(0);
        assertThat(catchThrowableOfType(ctx::commit, LifecycleFailure.class)
                       .getReason()).isEqualTo(Reason.INSERT_DELETE_EXISTING);
        assertThat(ctx.getState()).isEqualTo(State.ROLLED_BACK);
      }
    }

    @Test
    void testAntidependencyFailureOnRead() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = map.transact();
      ctx1.update(0, StringBox.of("zero_v1"));
      final var ctx2 = map.transact();
      assertThat(ctx2.read(0)).isEqualTo(StringBox.of("zero_v0")); // snapshot read

      ctx1.commit();
      assertThat(catchThrowable(ctx2::commit)).isExactlyInstanceOf(AntidependencyFailure.class);
    }

    @Test
    void testAntidependencyFailureOnReadAndWrite() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = map.transact();
      ctx1.update(0, StringBox.of("zero_v1"));
      final var ctx2 = map.transact();
      assertThat(ctx2.read(0)).isEqualTo(StringBox.of("zero_v0")); // snapshot read
      ctx2.update(0, StringBox.of("zero_v2"));

      ctx1.commit();
      assertThat(catchThrowable(ctx2::commit)).isExactlyInstanceOf(AntidependencyFailure.class);

      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v1"));
      }
    }

    @Test
    void testBlindWriteInCommitmentOrder() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = map.transact();
      ctx1.update(0, StringBox.of("zero_v1"));
      final var ctx2 = map.transact();
      ctx2.update(0, StringBox.of("zero_v2"));

      ctx1.commit();
      ctx2.commit();

      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v2"));
      }
    }
  }


}
