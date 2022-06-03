package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.util.*;

public final class RawVersioned {
  private final long version;

  private final DeepCloneable<?> value;

  public RawVersioned(long version, DeepCloneable<?> value) {
    this.version = version;
    this.value = value;
  }

  public long getVersion() {
    return version;
  }

  public boolean hasValue() {
    return value != null;
  }

  public DeepCloneable<?> getValue() {
    return value;
  }

  @Override
  public String toString() {
    return RawVersioned.class.getSimpleName()+ "[version=" + version +
        ", value=" + value + ']';
  }

  public <V extends DeepCloneable<V>> GenericVersioned<V> generify() {
    return new GenericVersioned<V>(version, Unsafe.cast(value));
  }
}
