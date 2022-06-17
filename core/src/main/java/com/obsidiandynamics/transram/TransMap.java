package com.obsidiandynamics.transram;

public interface TransMap<K, V extends DeepCloneable<V>> {
  TransContext<K, V> transact();

  Debug<K, V> debug();
}
