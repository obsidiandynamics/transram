package com.obsidiandynamics.transram;

public interface DeepCloneable<SELF> {
  SELF deepClone();

  static <T extends DeepCloneable<T>> DeepCloneable<T> clone(DeepCloneable<T> from) {
    return from != null ? from.deepClone() : null;
  }
}
