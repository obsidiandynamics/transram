package com.obsidiandynamics.transram.spec;

import java.util.function.Predicate;

public final class BiKey {
  private final int first;
  private final int second;

  public BiKey(int first, int second) {
    this.first = first;
    this.second = second;
  }

  public int getFirst() {
    return first;
  }

  public int getSecond() {
    return second;
  }

  @Override
  public int hashCode() {
    return (31 * 7 + first) * 7 + second;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o instanceof BiKey) {
      final var that = (BiKey) o;
      return first == that.first && second == that.second;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return BiKey.class.getSimpleName() + '[' + first + '_' + second + ']';
  }

  public static Predicate<BiKey> whereFirstIs(int first) {
    return key -> key.getFirst() == first;
  }
}
