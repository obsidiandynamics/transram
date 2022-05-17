package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.TransContext.*;

import java.util.function.*;

public final class Enclose<K, V extends DeepCloneable<V>> {
  public interface Region<K, V extends DeepCloneable<V>> {
    void perform(TransContext<K, V> ctx) throws ConcurrentModeFailure;
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
      try (var txn = map.transact()) {
        region.perform(txn);
        if (txn.getState() == State.ROLLED_BACK) {
          continue;
        }
        break;
      } catch (ConcurrentModeFailure concurrentModeFailure) {
        onFailure.accept(concurrentModeFailure);
      }
    }
  }
}
