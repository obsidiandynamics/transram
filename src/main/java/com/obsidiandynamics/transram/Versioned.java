package com.obsidiandynamics.transram;

public final class Versioned<V> {
  private final long version;

  private final V value;

  public Versioned(long version, V value) {
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

    final Versioned<?> versioned = (Versioned<?>) o;

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
    return Versioned.class.getSimpleName()+ "[version=" + version +
        ", value=" + value + ']';
  }

  public static <V> Versioned<V> unset() {
    return new Versioned<>(-1, null);
  }
}
