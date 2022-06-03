package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.util.*;

public interface Key {
  public static <K> KeyRef<K> wrap(K key) {
    Assert.that(key != null, () -> "Key cannot be null");
    return new KeyRef<>(key);
  }
}
