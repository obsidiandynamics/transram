package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.LifecycleFailure.*;
import com.obsidiandynamics.transram.SrmlMap.*;
import com.obsidiandynamics.transram.TransContext.*;
import com.obsidiandynamics.transram.mutex.*;
import org.junit.jupiter.api.*;
import org.mockito.*;

import static org.assertj.core.api.Assertions.*;

public final class SrmlContextTest extends AbstractContextTest {
  @Override
  <K, V extends DeepCloneable<V>> SrmlMap<K, V> newMap() {
    return newMap(new Options());
  }

  private static <K, V extends DeepCloneable<V>> SrmlMap<K, V> newMap(Options options) {
    return new SrmlMap<>(options);
  }

  @Nested
  class ValidationTests {
    @Test
    void testValidOptions() {
      assertThat(catchThrowableOfType(() -> newMap(new Options() {{
        mutexStripes = 0;
      }}), AssertionError.class)).hasMessage("Number of mutex stripes must exceed 0");

      assertThat(catchThrowableOfType(() -> newMap(new Options() {{
        queueDepth = 0;
      }}), AssertionError.class)).hasMessage("Queue depth must exceed 0");
    }
  }

  @Nested
  class AntidependencyTests {
    @Test
    void testAntidependencyFailureOnReadDueToWrite() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, StringBox>newMap();
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
    void testAntidependencyFailureOnReadAndWriteDueToWrite() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, StringBox>newMap();
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
    void testAntidependencyFailureOnReadDueToDelete() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = map.transact();
      ctx1.delete(0);
      final var ctx2 = map.transact();
      assertThat(ctx2.read(0)).isEqualTo(StringBox.of("zero_v0")); // snapshot read

      ctx1.commit();
      assertThat(catchThrowable(ctx2::commit)).isExactlyInstanceOf(AntidependencyFailure.class);
    }

    @Test
    void testBlindWriteInCommitmentOrder() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, StringBox>newMap();
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

    @Test
    void testAntidependencyFailureOnSizeCheckDueToResize() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, Nil>newMap();
      final var ctx1 = map.transact();
      assertThat(ctx1.size()).isEqualTo(0);

      final var ctx2 = map.transact();
      ctx2.insert(0, Nil.instance());
      ctx2.commit();

      assertThat(catchThrowable(ctx1::commit)).isExactlyInstanceOf(AntidependencyFailure.class);
    }

    @Test
    void testAntidependencyFailureOnKeyScanDueToResize() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, Nil>newMap();
      final var ctx1 = map.transact();
      assertThat(ctx1.keys(__ -> true)).isEmpty();

      final var ctx2 = map.transact();
      ctx2.insert(0, Nil.instance());
      ctx2.commit();

      assertThat(catchThrowable(ctx1::commit)).isExactlyInstanceOf(AntidependencyFailure.class);
    }
  }

  @Nested
  class SnapshotTests {
    @Test
    void testSnapshotRead() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.insert(1, StringBox.of("one_v0"));
        ctx.insert(2, StringBox.of("two_v0"));
        ctx.commit();
      }
      {
        final var ctx = map.transact();
        ctx.delete(2);
        ctx.commit();
      }

      final var ctx1 = map.transact();
      ctx1.update(0, StringBox.of("zero_v1"));
      ctx1.delete(1);
      ctx1.insert(2, StringBox.of("two_v1"));

      final var ctx2 = map.transact();
      ctx1.commit();

      assertThat(ctx2.read(0)).isEqualTo(StringBox.of("zero_v0"));
      assertThat(ctx2.read(1)).isEqualTo(StringBox.of("one_v0"));
      assertThat(ctx2.read(2)).isNull();
      assertThat(ctx2.size()).isEqualTo(2);
      assertThat(ctx2.keys(__ -> true)).containsExactly(0, 1);
    }

    @Test
    void testBrokenSnapshot() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.<Integer, StringBox>newMap(new Options() {{
        queueDepth = 1;
      }});
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = map.transact();
      ctx1.update(0, StringBox.of("zero_v1"));
      final var ctx2 = map.transact();
      ctx1.commit();
      assertThat(catchThrowable(() -> ctx2.read(0))).isExactlyInstanceOf(BrokenSnapshotFailure.class);
      assertThat(catchThrowable(() -> ctx2.keys(__ -> true))).isExactlyInstanceOf(BrokenSnapshotFailure.class);
    }
  }

  @Nested
  class InterruptTests {
    @Test
    void testInterruptOnReadCommit() throws ConcurrentModeFailure, InterruptedException {
      final var mutex = Mockito.mock(UpgradeableMutex.class);
      Mockito.doThrow(InterruptedException.class).when(mutex).tryReadAcquire(Mockito.anyLong());
      final var map = SrmlContextTest.<Integer, Nil>newMap(new Options() {{
        mutexFactory = () -> mutex;
      }});
      final var ctx = map.transact();
      ctx.read(0);

      assertThat(catchThrowableOfType(ctx::commit, MutexAcquisitionFailure.class)).hasMessage("Interrupted while acquiring read lock");
    }

    @Test
    void testInterruptOnWriteCommit() throws ConcurrentModeFailure, InterruptedException {
      final var mutex = Mockito.mock(UpgradeableMutex.class);
      Mockito.doThrow(InterruptedException.class).when(mutex).tryWriteAcquire(Mockito.anyLong());
      final var map = SrmlContextTest.<Integer, Nil>newMap(new Options() {{
        mutexFactory = () -> mutex;
      }});
      final var ctx = map.transact();
      ctx.insert(0, Nil.instance());

      assertThat(catchThrowableOfType(ctx::commit, MutexAcquisitionFailure.class)).hasMessage("Interrupted while acquiring write lock");
    }
  }

  @Nested
  class TombstoneTests {
    @Test
    void testInsertDeleteOfNonexistentOnTombstone() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, Nil>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, Nil.instance());
        ctx.commit();
      }
      {
        final var ctx = map.transact();
        ctx.delete(0);
        ctx.commit();
      }
      {
        final var ctx = map.transact();
        ctx.insert(0, Nil.instance());
        ctx.delete(0);
        ctx.commit();
      }
      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isNull();
      }
    }

    @Test
    void testDeleteOfNonexistentOnTombstone() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, Nil>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(1, Nil.instance());
        ctx.commit();
      }
      {
        final var ctx = map.transact();
        ctx.delete(1);
        ctx.commit();
      }
      {
        final var ctx = map.transact();
        ctx.insert(0, Nil.instance()); // inserting an item here prevents negative size upon later delete
        ctx.delete(1);
        assertThat(catchThrowableOfType(ctx::commit, LifecycleFailure.class)
                       .getReason()).isEqualTo(Reason.DELETE_NONEXISTENT);
        assertThat(ctx.getState()).isEqualTo(State.ROLLED_BACK);
      }
    }
  }
}
