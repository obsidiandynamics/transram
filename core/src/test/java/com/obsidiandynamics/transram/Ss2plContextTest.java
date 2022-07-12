package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.Ss2plMap.*;
import com.obsidiandynamics.transram.mutex.*;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

public final class Ss2plContextTest extends AbstractContextTest {
  @Override
  <K, V extends DeepCloneable<V>> Ss2plMap<K, V> newMap() {
    return newMap(new Options());
  }

  private static <K, V extends DeepCloneable<V>> Ss2plMap<K, V> newMap(Options options) {
    return new Ss2plMap<>(options);
  }

  @Nested
  class ValidationTests {
    @Test
    void testValidOptions() {
      assertThat(catchThrowableOfType(() -> newMap(new Options() {{
        mutexStripes = 0;
      }}), AssertionError.class)).hasMessage("Number of mutex stripes must exceed 0");

      assertThat(catchThrowableOfType(() -> newMap(new Options() {{
        mutexTimeoutMs = -1;
      }}), AssertionError.class)).hasMessage("Mutex timeout must be equal to or greater than 0");
    }
  }

  @Nested
  class MutexTests {
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
    void testMutexFailureOnReadDueToWrite() throws ConcurrentModeFailure {
      final var map = Ss2plContextTest.this.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = threaded(map.transact());
      ctx1.update(0, StringBox.of("zero_v1"));
      final var ctx2 = threaded(map.transact());
      assertThat(catchThrowable(() -> ctx2.read(0))).isExactlyInstanceOf(MutexAcquisitionFailure.class);
    }

    @Test
    void testMutexFailureOnWriteDueToRead() throws ConcurrentModeFailure {
      final var map = Ss2plContextTest.this.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = threaded(map.transact());
      ctx1.read(0);
      final var ctx2 = threaded(map.transact());
      assertThat(catchThrowable(() -> ctx2.update(0, StringBox.of("zero_v1")))).isExactlyInstanceOf(MutexAcquisitionFailure.class);
    }

    @Test
    void testMutexFailureOnWriteDueToWrite() throws ConcurrentModeFailure {
      final var map = Ss2plContextTest.this.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = threaded(map.transact());
      ctx1.update(0, StringBox.of("zero_v1"));
      final var ctx2 = threaded(map.transact());
      assertThat(catchThrowable(() -> ctx2.update(0, StringBox.of("zero_v2")))).isExactlyInstanceOf(MutexAcquisitionFailure.class);
    }

    @Test
    void testMutexFailureOnUpgradeDueToRead() throws ConcurrentModeFailure {
      final var map = Ss2plContextTest.this.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = threaded(map.transact());
      ctx1.read(0);
      final var ctx2 = threaded(map.transact());
      assertThat(ctx2.read(0)).isEqualTo(StringBox.of("zero_v0"));
      assertThat(catchThrowable(() -> ctx2.update(0, StringBox.of("zero_v1")))).isExactlyInstanceOf(MutexAcquisitionFailure.class);
    }

    @Test
    void testMutexFailureOnReadDueToDelete() throws ConcurrentModeFailure {
      final var map = Ss2plContextTest.this.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = threaded(map.transact());
      ctx1.delete(0);
      final var ctx2 = threaded(map.transact());
      assertThat(catchThrowable(() -> ctx2.read(0))).isExactlyInstanceOf(MutexAcquisitionFailure.class);
    }

    @Test
    void testMutexFailureOnSizeCheckDueToResize() throws ConcurrentModeFailure {
      final var map = Ss2plContextTest.this.<Integer, Nil>newMap();
      final var ctx1 = threaded(map.transact());
      ctx1.insert(0, Nil.instance());

      final var ctx2 = threaded(map.transact());
      assertThat(catchThrowable(ctx2::size)).isExactlyInstanceOf(MutexAcquisitionFailure.class);
    }

    @Test
    void testMutexFailureOnKeyScanDueToResize() throws ConcurrentModeFailure {
      final var map = Ss2plContextTest.this.<Integer, Nil>newMap();
      final var ctx1 = threaded(map.transact());
      ctx1.insert(0, Nil.instance());

      final var ctx2 = threaded(map.transact());
      assertThat(catchThrowable(() -> ctx2.keys(__ -> true))).isExactlyInstanceOf(MutexAcquisitionFailure.class);
    }
  }

  @Nested
  class InterruptTests {
    @Test
    void testInterruptOnRead() throws InterruptedException {
      final var mutex = Mockito.mock(UpgradeableMutex.class);
      Mockito.doThrow(InterruptedException.class).when(mutex).tryReadAcquire(Mockito.anyLong());
      final var map = Ss2plContextTest.<Integer, Nil>newMap(new Ss2plMap.Options() {{
        mutexFactory = () -> mutex;
      }});
      final var ctx = map.transact();
      assertThat(catchThrowableOfType(() -> ctx.read(0), MutexAcquisitionFailure.class)).hasMessage("Interrupted while acquiring read mutex");
    }

    @Test
    void testInterruptOnWrite() throws InterruptedException {
      final var mutex = Mockito.mock(UpgradeableMutex.class);
      Mockito.doThrow(InterruptedException.class).when(mutex).tryWriteAcquire(Mockito.anyLong());
      final var map = Ss2plContextTest.<Integer, Nil>newMap(new Ss2plMap.Options() {{
        mutexFactory = () -> mutex;
      }});
      final var ctx = map.transact();
      assertThat(catchThrowableOfType(() -> ctx.update(0, Nil.instance()), MutexAcquisitionFailure.class)).hasMessage("Interrupted while acquiring write mutex");
    }

    @Test
    void testInterruptOnUpgrade() throws ConcurrentModeFailure, InterruptedException {
      final var mutex = Mockito.mock(UpgradeableMutex.class);
      Mockito.doReturn(true).when(mutex).tryReadAcquire(Mockito.anyLong());
      Mockito.doThrow(InterruptedException.class).when(mutex).tryUpgrade(Mockito.anyLong());
      final var map = Ss2plContextTest.<Integer, Nil>newMap(new Ss2plMap.Options() {{
        mutexFactory = () -> mutex;
      }});
      final var ctx = map.transact();
      assertThat(ctx.read(0)).isNull();
      assertThat(catchThrowableOfType(() -> ctx.insert(0, Nil.instance()), MutexAcquisitionFailure.class)).hasMessage("Interrupted while upgrading mutex");
    }
  }
}
