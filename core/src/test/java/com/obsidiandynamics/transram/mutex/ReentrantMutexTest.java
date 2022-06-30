package com.obsidiandynamics.transram.mutex;

import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

final class ReentrantMutexTest {
  @Nested
  class IllegalMonitorStateTests {
    @Test
    void testReadAfterRead() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
    }

    @Test
    void testWriteAfterRead() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(mutex.tryWriteAcquire(0)).isFalse();
      assertThat(mutex.tryWriteAcquire(1)).isFalse();
    }

    @Test
    void testDowngradeAfterRead() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(catchException(mutex::downgrade))
          .isInstanceOf(IllegalMonitorStateException.class);
    }

    @Test
    void testReadAfterWrite() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      assertThat(mutex.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
    }

    @Test
    void testWriteAfterWrite() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      assertThat(mutex.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(mutex.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
    }

    @Test
    void testDowngradeWithoutWrite() {
      final var mutex = new ReentrantMutex(false);
      assertThat(catchException(mutex::downgrade))
          .isInstanceOf(IllegalMonitorStateException.class);
    }

    @Test
    void testReadReleaseWithoutRead() {
      final var mutex = new ReentrantMutex(false);
      assertThat(catchException(mutex::readRelease))
          .isInstanceOf(IllegalMonitorStateException.class);
    }

    @Test
    void testWriteReleaseWithoutWrite() {
      final var mutex = new ReentrantMutex(false);
      assertThat(catchException(mutex::writeRelease))
          .isInstanceOf(IllegalMonitorStateException.class);
    }
  }

  @Nested
  class CycleTests {
    @Test
    void testReadReleaseCycle() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      for (var i = 0; i < 2; i++) {
        assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
        mutex.readRelease();
      }
    }

    @Test
    void testWriteReleaseCycle() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      for (var i = 0; i < 2; i++) {
        assertThat(mutex.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
        mutex.writeRelease();
      }
    }

    @Test
    void testWriteDowngradeReleaseCycle() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      for (var i = 0; i < 2; i++) {
        assertThat(mutex.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
        mutex.downgrade();
        mutex.readRelease();
      }
    }
  }

  @Nested
  class ThreadedTests {
    private static final long SHORT_SLEEP_MS = 1;

    private List<ExecutorService> executors;

    @BeforeEach
    void beforeEach() {
      executors = new ArrayList<>();
    }

    @AfterEach
    void afterEach() {
      executors.forEach(ExecutorService::shutdown);
    }

    ThreadedMutex threaded(Mutex delegate) {
      final var executor = Executors.newSingleThreadExecutor();
      executors.add(executor);
      return new ThreadedMutex(delegate, executor);
    }

    @Test
    void testReadAcquireWhileReadLocked() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      m1.readRelease();
      m2.readRelease();
    }

    @Test
    void testTimeoutOnWriteAcquireWhileReadLocked() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryWriteAcquire(0)).isFalse();
      assertThat(m2.tryWriteAcquire(1)).isFalse();
    }

    @Test
    void testTimeoutOnWriteAcquireWhileWriteLocked() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryWriteAcquire(0)).isFalse();
      assertThat(m2.tryWriteAcquire(1)).isFalse();
    }

    @Test
    void testTimeoutOnReadAcquireWhileWriteLocked() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryReadAcquire(0)).isFalse();
      assertThat(m2.tryReadAcquire(1)).isFalse();
    }

    @Test
    void testAwaitWriteAcquireWhileReadLocked() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      final var m2_tryWriteAcquire = m2.tryWriteAcquireAsync(Long.MAX_VALUE);
      Thread.sleep(SHORT_SLEEP_MS);
      assertThat(m2_tryWriteAcquire.completable().isDone()).isFalse();
      m1.readRelease();
      assertThat(m2_tryWriteAcquire.get()).isTrue();
    }

    @Test
    void testAwaitWriteAcquireWhileLockedBySeveralReaders() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      final var m3 = threaded(mutex);
      assertThat(m1.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      final var m3_tryWriteAcquire = m3.tryWriteAcquireAsync(Long.MAX_VALUE);
      Thread.sleep(SHORT_SLEEP_MS);
      assertThat(m3_tryWriteAcquire.completable().isDone()).isFalse();
      m1.readRelease();
      Thread.sleep(SHORT_SLEEP_MS);
      assertThat(m3_tryWriteAcquire.completable().isDone()).isFalse();
      m2.readRelease();
      assertThat(m3_tryWriteAcquire.get()).isTrue();
    }

    @Test
    void testAwaitReadAcquireWhileWriteLocked() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
      final var m2_tryReadAcquire = m2.tryReadAcquireAsync(Long.MAX_VALUE);
      Thread.sleep(SHORT_SLEEP_MS);
      assertThat(m2_tryReadAcquire.completable().isDone()).isFalse();
      m1.writeRelease();
      assertThat(m2_tryReadAcquire.get()).isTrue();
    }

    @Test
    void testAwaitReadAcquireWhileWriteLockedWithDowngrade() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
      final var m2_tryReadAcquire = m2.tryReadAcquireAsync(Long.MAX_VALUE);
      Thread.sleep(SHORT_SLEEP_MS);
      assertThat(m2_tryReadAcquire.completable().isDone()).isFalse();
      m1.downgrade();
      assertThat(m2_tryReadAcquire.get()).isTrue();
    }

    @Test
    void testCompetingMultipleWriteAcquireWhileReadLocked() throws InterruptedException {
      final var mutex = new ReentrantMutex(false);
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      final var m3 = threaded(mutex);
      assertThat(m1.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      final var m2_tryWriteAcquire = m2.tryWriteAcquireAsync(Long.MAX_VALUE);
      final var m3_tryWriteAcquire = m3.tryWriteAcquireAsync(Long.MAX_VALUE);

      // neither m2 nor m3 may proceed initially
      Thread.sleep(SHORT_SLEEP_MS);
      assertThat(m2_tryWriteAcquire.completable().isDone()).isFalse();
      assertThat(m3_tryWriteAcquire.completable().isDone()).isFalse();

      m1.readRelease();
      // after read-release, exactly one of m2 or m3 will unblock
      CompletableFuture.anyOf(m2_tryWriteAcquire.completable(), m3_tryWriteAcquire.completable()).join();
      Thread.sleep(SHORT_SLEEP_MS);
      assertThat(m2_tryWriteAcquire.completable().isDone() ^ m3_tryWriteAcquire.completable().isDone()).isTrue();
      if (m2_tryWriteAcquire.completable().isDone()) {
        assertThat(m2_tryWriteAcquire.get()).isTrue();
        m2.writeRelease();
        assertThat(m3_tryWriteAcquire.get()).isTrue();
      } else {
        assertThat(m3_tryWriteAcquire.get()).isTrue();
        m3.writeRelease();
        assertThat(m2_tryWriteAcquire.get()).isTrue();
      }
    }
  }
}
