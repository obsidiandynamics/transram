package com.obsidiandynamics.transram;

final class CollidingInteger {
  private final int value;

  private CollidingInteger(int value) {
    this.value = value;
  }

  static CollidingInteger of(int value) {
    return new CollidingInteger(value);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof CollidingInteger && ((CollidingInteger) o).value == value;
  }

  @Override
  public String toString() {
    return CollidingInteger.class.getSimpleName() + "[" + value + ']';
  }
}
