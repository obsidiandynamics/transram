package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.util.*;

public final class RawVersioned implements DeepCloneable<RawVersioned> {
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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final RawVersioned versioned = (RawVersioned) o;

    if (version != versioned.version) return false;
    return value.equals(versioned.value);
  }

  @Override
  public int hashCode() {
    int result = (int) (version ^ (version >>> 32));
    result = 31 * result + value.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return RawVersioned.class.getSimpleName()+ "[version=" + version +
        ", value=" + value + ']';
  }

//  public static RawVersioned unset() {
//    return new RawVersioned(-1, null);
//  }

  @Override
  public RawVersioned deepClone() {
    return value == null ? this : new RawVersioned(version, (DeepCloneable<?>) value.deepClone());
  }

  public <V extends DeepCloneable<V>> GenericVersioned<V> generify() {
    return new GenericVersioned<V>(version, Unsafe.cast(value));
  }
}
