package com.obsidiandynamics.transram.mutex;

import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

final class UnfairUpgradeableMutexTest {
  @Nested
  class IllegalMonitorStateTests {
    @Test
    void testReadAfterRead() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(catchException(() -> mutex.tryReadAcquire(Long.MAX_VALUE)))
          .isInstanceOf(IllegalMonitorStateException.class).hasMessage("Already read-locked");
    }

    @Test
    void testWriteAfterRead() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(catchException(() -> mutex.tryWriteAcquire(Long.MAX_VALUE)))
          .isInstanceOf(IllegalMonitorStateException.class).hasMessage("Already read-locked, use upgrade methods");
    }

    @Test
    void testDowngradeAfterRead() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(catchException(mutex::downgrade))
          .isInstanceOf(IllegalMonitorStateException.class).hasMessage("Not write-locked");
    }

    @Test
    void testReadAfterWrite() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      assertThat(mutex.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(catchException(() -> mutex.tryReadAcquire(Long.MAX_VALUE)))
          .isInstanceOf(IllegalMonitorStateException.class).hasMessage("Already write-locked, use downgrade method");
    }

    @Test
    void testWriteAfterWrite() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      assertThat(mutex.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(catchException(() -> mutex.tryWriteAcquire(Long.MAX_VALUE)))
          .isInstanceOf(IllegalMonitorStateException.class).hasMessage("Already write-locked");
    }

    @Test
    void testUpgradeAfterWrite() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      assertThat(mutex.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(catchException(() -> mutex.tryUpgrade(Long.MAX_VALUE)))
          .isInstanceOf(IllegalMonitorStateException.class).hasMessage("Not read-locked");
    }

    @Test
    void testUpgradeWithoutRead() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      assertThat(catchException(() -> mutex.tryUpgrade(Long.MAX_VALUE)))
          .isInstanceOf(IllegalMonitorStateException.class).hasMessage("Not read-locked");
    }

    @Test
    void testDowngradeWithoutWrite() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      assertThat(catchException(mutex::downgrade))
          .isInstanceOf(IllegalMonitorStateException.class).hasMessage("Not write-locked");
    }

    @Test
    void testReadReleaseWithoutRead() {
      final var mutex = new UnfairUpgradeableMutex();
      assertThat(catchException(mutex::readRelease))
          .isInstanceOf(IllegalMonitorStateException.class).hasMessage("Not read-locked");
    }

    @Test
    void testWriteReleaseWithoutWrite() {
      final var mutex = new UnfairUpgradeableMutex();
      assertThat(catchException(mutex::writeRelease))
          .isInstanceOf(IllegalMonitorStateException.class).hasMessage("Not write-locked");
    }
  }

  @Nested
  class CycleTests {
    @Test
    void testReadReleaseCycle() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      for (var i = 0; i < 2; i++) {
        assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
        mutex.readRelease();
      }
    }

    @Test
    void testReadUpgradeReleaseCycle() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      for (var i = 0; i < 2; i++) {
        assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
        assertThat(mutex.tryUpgrade(Long.MAX_VALUE)).isTrue();
        mutex.writeRelease();
      }
    }

    @Test
    void testReadUpgradeDowngradeReleaseCycle() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      for (var i = 0; i < 2; i++) {
        assertThat(mutex.tryReadAcquire(Long.MAX_VALUE)).isTrue();
        assertThat(mutex.tryUpgrade(Long.MAX_VALUE)).isTrue();
        mutex.downgrade();
        mutex.readRelease();
      }
    }

    @Test
    void testWriteReleaseCycle() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      for (var i = 0; i < 2; i++) {
        assertThat(mutex.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
        mutex.writeRelease();
      }
    }

    @Test
    void testWriteDowngradeReleaseCycle() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      for (var i = 0; i < 2; i++) {
        assertThat(mutex.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
        mutex.downgrade();
        mutex.readRelease();
      }
    }

    @Test
    void testWriteDowngradeUpgradeReleaseCycle() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      for (var i = 0; i < 2; i++) {
        assertThat(mutex.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
        mutex.downgrade();
        assertThat(mutex.tryUpgrade(Long.MAX_VALUE)).isTrue();
        mutex.writeRelease();
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

    ThreadedUpgradeableMutex threaded(UpgradeableMutex delegate) {
      final var executor = Executors.newSingleThreadExecutor();
      executors.add(executor);
      return new ThreadedUpgradeableMutex(delegate, executor);
    }

    @Test
    void testReadAcquireWhileReadLocked() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      m1.readRelease();
      m2.readRelease();
    }

    @Test
    void testTimeoutOnWriteAcquireWhileReadLocked() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryWriteAcquire(0)).isFalse();
      assertThat(m2.tryWriteAcquire(1)).isFalse();
    }

    @Test
    void testTimeoutOnUpgradeWhileReadLocked() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryUpgrade(0)).isFalse();
      assertThat(m2.tryUpgrade(1)).isFalse();
    }

    @Test
    void testTimeoutOnWriteAcquireWhileWriteLocked() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryWriteAcquire(0)).isFalse();
      assertThat(m2.tryWriteAcquire(1)).isFalse();
    }

    @Test
    void testTimeoutOnReadAcquireWhileWriteLocked() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryWriteAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryReadAcquire(0)).isFalse();
      assertThat(m2.tryReadAcquire(1)).isFalse();
    }

    @Test
    void testAwaitWriteAcquireWhileReadLocked() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
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
      final var mutex = new UnfairUpgradeableMutex();
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
    void testAwaitUpgradeWhileReadLocked() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      assertThat(m1.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      final var m2_tryUpgrade = m2.tryUpgradeAsync(Long.MAX_VALUE);
      Thread.sleep(SHORT_SLEEP_MS);
      assertThat(m2_tryUpgrade.completable().isDone()).isFalse();
      m1.readRelease();
      assertThat(m2_tryUpgrade.get()).isTrue();
    }

    @Test
    void testAwaitUpgradeWhileLockedBySeveralReaders() throws InterruptedException {
      final var mutex = new UnfairUpgradeableMutex();
      final var m1 = threaded(mutex);
      final var m2 = threaded(mutex);
      final var m3 = threaded(mutex);
      assertThat(m1.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m2.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      assertThat(m3.tryReadAcquire(Long.MAX_VALUE)).isTrue();
      final var m3_tryUpgrade = m3.tryUpgradeAsync(Long.MAX_VALUE);
      Thread.sleep(SHORT_SLEEP_MS);
      assertThat(m3_tryUpgrade.completable().isDone()).isFalse();
      m1.readRelease();
      Thread.sleep(SHORT_SLEEP_MS);
      assertThat(m3_tryUpgrade.completable().isDone()).isFalse();
      m2.readRelease();
      assertThat(m3_tryUpgrade.get()).isTrue();
    }
  }
}
