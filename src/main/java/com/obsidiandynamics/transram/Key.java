package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.util.*;

interface Key {
  static <K> KeyRef<K> wrap(K key) {
    Assert.that(key != null, NullKeyAssertionError::new, () -> "Key cannot be null");
    return new KeyRef<>(key);
  }
}
