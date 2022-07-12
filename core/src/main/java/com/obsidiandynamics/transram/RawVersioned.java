package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.util.*;

final class RawVersioned {
  private final long version;

  private final DeepCloneable<?> value;

  RawVersioned(long version, DeepCloneable<?> value) {
    this.version = version;
    this.value = value;
  }

  long getVersion() {
    return version;
  }

  boolean hasValue() {
    return value != null;
  }

  DeepCloneable<?> getValue() {
    return value;
  }

  @Override
  public String toString() {
    return RawVersioned.class.getSimpleName()+ "[version=" + version +
        ", value=" + value + ']';
  }

  <V extends DeepCloneable<V>> GenericVersioned<V> generify() {
    return new GenericVersioned<>(version, Unsafe.cast(value));
  }
}
