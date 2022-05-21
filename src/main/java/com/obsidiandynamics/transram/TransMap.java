package com.obsidiandynamics.transram;

import java.util.*;

public interface TransMap<K, V extends DeepCloneable<V>> {
  TransContext<K, V> transact();

  Debug<K, V> debug();
}
