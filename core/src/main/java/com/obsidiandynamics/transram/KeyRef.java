package com.obsidiandynamics.transram;

final class KeyRef<K> implements Key {
  private final K key;

  KeyRef(K key) {
    this.key = key;
  }

  K unwrap() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof KeyRef<?>) {
      final var other = (KeyRef<?>) o;
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
    return KeyRef.class.getSimpleName() + '[' + key.toString() + ']';
  }
}
