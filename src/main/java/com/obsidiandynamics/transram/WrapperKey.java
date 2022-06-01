package com.obsidiandynamics.transram;

public final class WrapperKey<K> implements Key {
  private final K key;

  private WrapperKey(K key) {
    this.key = key;
  }

  public static <K> WrapperKey<K> wrap(K key) {
    return new WrapperKey<>(key);
  }

  public K unwrap() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof  WrapperKey<?>) {
      final var other = (WrapperKey<?>) o;
      return key.equals(other.key);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public String toString() {
    return WrapperKey.class.getSimpleName() + '[' + key.toString() + ']';
  }
}
