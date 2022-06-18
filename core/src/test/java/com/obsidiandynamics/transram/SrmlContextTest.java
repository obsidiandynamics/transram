package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.LifecycleFailure.*;
import com.obsidiandynamics.transram.SrmlMap.*;
import com.obsidiandynamics.transram.ThreadedContext.*;
import com.obsidiandynamics.transram.TransContext.*;
import com.obsidiandynamics.transram.mutex.*;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.*;
import java.util.concurrent.*;

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

  @Nested
  class QueueDrainTests {
    private List<ExecutorService> executors;

    @BeforeEach
    void beforeEach() {
      executors = new ArrayList<>();
    }

    @AfterEach
    void afterEach() {
      executors.forEach(ExecutorService::shutdown);
    }

    <K, V extends DeepCloneable<V>> ThreadedContext<K, V> threaded(TransContext<K, V> delegate) {
      final var executor = Executors.newSingleThreadExecutor();
      executors.add(executor);
      return new ThreadedContext<>(delegate, executor);
    }

    @Test
    void testOvertakenByCommitted() throws ConcurrentModeFailure {
      final var map = Mockito.spy(SrmlContextTest.this.<Integer, StringBox>newMap());
      final var store = Mockito.spy(new ConcurrentHashMap<Key, Deque<RawVersioned>>());
      store.putAll(map.getStore());
      Mockito.doReturn(store).when(map).getStore();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.insert(1, StringBox.of("zero_v0"));
        ctx.commit();
      }

      Mockito.doAnswer(invocation -> {
        final var key = invocation.getArgument(0, Key.class);
        final var ctx2 = map.transact();
        ctx2.update(1, StringBox.of("one_v1"));
        ctx2.commit();
        assertThat(ctx2.getState()).isEqualTo(State.COMMITTED);
        return invocation.callRealMethod();
      }).when(store).compute(Mockito.eq(Key.wrap(0)), Mockito.any());

      final var ctx1 = map.transact();
      ctx1.update(0, StringBox.of("zero_v1"));
      ctx1.commit();
      assertThat(ctx1.getState()).isEqualTo(State.COMMITTED);

      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v1"));
        assertThat(ctx.read(1)).isEqualTo(StringBox.of("one_v1"));
      }
    }

    @Test
    void testTwoTransactionsSimultaneouslyAttemptToRemoveQueuedContext() throws ConcurrentModeFailure {
      final var map = Mockito.spy(SrmlContextTest.this.<Integer, StringBox>newMap());
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.insert(1, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = threaded(map.transact());
      ctx1.update(0, StringBox.of("zero_v1"));

      final var ctx2 = threaded(map.transact());
      ctx2.update(1, StringBox.of("one_v1"));

      final var queuedContexts = Mockito.spy(map.getQueuedContexts());
      Mockito.doReturn(queuedContexts).when(map).getQueuedContexts();
      final var barrier = new CyclicBarrier(2);
      Mockito.doAnswer(invocation -> {
        barrier.await();
        return invocation.callRealMethod();
      }).when(queuedContexts).peekFirst();

      final var future1 = ctx1.commitFuture();
      final var future2 = ctx2.commitFuture();
      CompletableFuture.allOf(future1.completable(), future2.completable()).join();

      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v1"));
        assertThat(ctx.read(1)).isEqualTo(StringBox.of("one_v1"));
      }
    }
  }
}
