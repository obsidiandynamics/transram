package com.obsidiandynamics.transram;

import java.util.*;

public final class GenericVersioned<V extends DeepCloneable<V>> implements DeepCloneable<GenericVersioned<V>> {
  private final long version;

  private final V value;

  public GenericVersioned(long version, V value) {
    this.version = version;
    this.value = value;
  }

  public long getVersion() {
    return version;
  }

  public boolean hasValue() {
    return value != null;
  }

  public V getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final var that = (GenericVersioned<?>) o;
    if (version != that.version) return false;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    int result = (int) (version ^ (version >>> 32));
    result = 31 * result + Objects.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    return GenericVersioned.class.getSimpleName()+ "[version=" + version +
        ", value=" + value + ']';
  }

  @Override
  public GenericVersioned<V> deepClone() {
    return value == null ? this : new GenericVersioned<>(version, value.deepClone());
  }
}
