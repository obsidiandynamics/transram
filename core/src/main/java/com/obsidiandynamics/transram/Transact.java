package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.TransContext.*;
import com.obsidiandynamics.transram.Transact.Region.*;

import java.util.function.*;

public final class Transact<K, V extends DeepCloneable<V>> {
  public interface Region<K, V extends DeepCloneable<V>> {
    enum Action {
      ROLLBACK_AND_RESET,
      ROLLBACK,
      COMMIT
    }

    Action perform(TransContext<K, V> ctx) throws ConcurrentModeFailure;
  }

  private final TransMap<K, V> map;

  private Consumer<ConcurrentModeFailure> onFailure = __ -> {};

  private Transact(TransMap<K, V> map) {
    this.map = map;
  }

  public Transact<K, V> withFailureHandler(Consumer<ConcurrentModeFailure> onFailure) {
    this.onFailure = onFailure;
    return this;
  }

  public TransContext<K, V> run(Region<K, V> region) {
    return run(map, region, onFailure);
  }

  public static <K, V extends DeepCloneable<V>> Transact<K, V> over(TransMap<K, V> map) {
    return new Transact<>(map);
  }

  public static final class RuntimeInterruptedException extends RuntimeException {
    RuntimeInterruptedException(InterruptedException cause) { super(cause); }
  }

  public static <K, V extends DeepCloneable<V>> TransContext<K, V> run(TransMap<K, V> map, Region<K, V> region, Consumer<ConcurrentModeFailure> onFailure) {
    var maxBackoffMillis = 0;
    while (true) {
      try {
        final var ctx = map.transact();
        final var outcome = region.perform(ctx);
        switch (outcome) {
          case ROLLBACK_AND_RESET:
            if (ctx.getState() != State.ROLLED_BACK) ctx.rollback();
            break;
          case ROLLBACK:
            if (ctx.getState() != State.ROLLED_BACK) ctx.rollback();
            return ctx;
          case COMMIT:
            if (ctx.getState() != State.COMMITTED) ctx.commit();
            return ctx;
        }
      } catch (ConcurrentModeFailure concurrentModeFailure) {
        onFailure.accept(concurrentModeFailure);
        final var rnd = Math.random();
        try {
          //noinspection BusyWait
          Thread.sleep((int) (rnd * maxBackoffMillis), (int) (rnd * 1_000_000));
          maxBackoffMillis++;
        } catch (InterruptedException e) {
          throw new RuntimeInterruptedException(e);
        }
      }
    }
  }
}
