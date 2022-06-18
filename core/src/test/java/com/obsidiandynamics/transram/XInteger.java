package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.AbstractContextTest.*;

final class XInteger {
  private final int value;

  private XInteger(int value) {
    this.value = value;
  }

  static XInteger of(int value) {
    return new XInteger(value);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof XInteger && ((XInteger) o).value == value;
  }

  @Override
  public String toString() {
    return XInteger.class.getSimpleName() + "[" + value + ']';
  }
}
