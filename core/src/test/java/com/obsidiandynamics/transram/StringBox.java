package com.obsidiandynamics.transram;

import java.util.*;

public class StringBox implements DeepCloneable<StringBox> {
  private String value;

  private StringBox(String value) {
    this.value = value;
  }

  public static StringBox of(String value) {
    return new StringBox(value);
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return StringBox.class.getSimpleName() + '[' + value + ']';
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o instanceof StringBox) {
      final var that = (StringBox) o;
      return Objects.equals(value, that.value);
    } else {
      return false;
    }
  }

  @Override
  public StringBox deepClone() {
    return new StringBox(value);
  }
}
