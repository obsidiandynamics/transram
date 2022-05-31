package com.obsidiandynamics.transram.util;

import java.util.concurrent.*;
import java.util.function.*;

public final class TimedRunner {
  public static Executor inNewThread() {
    return runnable -> {
      new Thread(runnable).start();
    };
  };

  public static <T> long run(int threads, int initOpsPerThread, long minDurationMs, Executor executor, Supplier<T> threadLocalInit, Consumer<T> runnable) throws InterruptedException {
    final var latch = new CountDownLatch(threads);
    final var startTime = System.currentTimeMillis();
    for (var thread = 0; thread < threads; thread++) {
      executor.execute(() -> {
        final var threadLocal = threadLocalInit.get();
        var opsPerThread = initOpsPerThread;
        var op = 0;
        while (true) {
          runnable.accept(threadLocal);
          if (++op == opsPerThread) {
            final var took = System.currentTimeMillis() - startTime;
            if (took < minDurationMs) {
              final var targetOpsPerThread = (long) ((double) opsPerThread * minDurationMs / took);
              opsPerThread += Math.max(1, (targetOpsPerThread - opsPerThread) / 2);
            } else {
              break;
            }
          }
        }
        latch.countDown();
      });
    }

    latch.await();
    return System.currentTimeMillis() - startTime;
  }
}
