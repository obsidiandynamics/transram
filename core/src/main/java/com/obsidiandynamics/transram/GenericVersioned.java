package com.obsidiandynamics.transram;

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

    final GenericVersioned<?> versioned = (GenericVersioned<?>) o;

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
    return GenericVersioned.class.getSimpleName()+ "[version=" + version +
        ", value=" + value + ']';
  }

  public static <V extends DeepCloneable<V>> GenericVersioned<V> unset() {
    return new GenericVersioned<>(-1, null);
  }

  @Override
  public GenericVersioned<V> deepClone() {
    return value == null ? this : new GenericVersioned<>(version, value.deepClone());
  }
}
