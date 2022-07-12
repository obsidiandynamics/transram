package com.obsidiandynamics.transram.util;

import java.util.concurrent.*;
import java.util.function.*;

public final class TimedRunner {
  /**
   * Runs a task in a new thread.
   * @return An {@link Executor}.
   */
  public static Executor inNewThread() {
    return runnable -> new Thread(runnable).start();
  }

  public static <T> long run(int threads, int initialIterations, long minDurationMs, Executor executor, Supplier<T> threadLocalInit, Consumer<T> iteration) throws InterruptedException {
    final var latch = new CountDownLatch(threads);
    final var startTime = System.currentTimeMillis();
    for (var thread = 0; thread < threads; thread++) {
      executor.execute(() -> {
        try {
          final var threadLocal = threadLocalInit.get();
          var opsPerThread = initialIterations;
          var op = 0;
          while (true) {
            iteration.accept(threadLocal);
            if (++op == opsPerThread) {
              final var took = System.currentTimeMillis() - startTime;
              if (took < minDurationMs) {
                final var targetOpsPerThread = (long) ((double) opsPerThread * minDurationMs / took);
                opsPerThread += Math.max(1, (targetOpsPerThread - opsPerThread) * .1);
              } else {
                break;
              }
            }
          }
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await();
    return System.currentTimeMillis() - startTime;
  }
}
