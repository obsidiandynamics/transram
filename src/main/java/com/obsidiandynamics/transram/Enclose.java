package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.TransContext.*;

import java.util.function.*;

public final class Enclose<K, V extends DeepCloneable<V>> {
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

  private Enclose(TransMap<K, V> map) {
    this.map = map;
  }

  public Enclose<K, V> onFailure(Consumer<ConcurrentModeFailure> onFailure) {
    this.onFailure = onFailure;
    return this;
  }

  public void transact(Region<K, V> region) {
    transact(map, region, onFailure);
  }

  public static <K, V extends DeepCloneable<V>> Enclose<K, V> over(TransMap<K, V> map) {
    return new Enclose<>(map);
  }

  public static <K, V extends DeepCloneable<V>> void transact(TransMap<K, V> map, Region<K, V> region, Consumer<ConcurrentModeFailure> onFailure) {
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
              return;
            case COMMIT:
              if (ctx.getState() != State.COMMITTED) ctx.commit();
              break;
          }
          break;
        } catch (ConcurrentModeFailure concurrentModeFailure) {
          onFailure.accept(concurrentModeFailure);
        }
      }
  }
}
