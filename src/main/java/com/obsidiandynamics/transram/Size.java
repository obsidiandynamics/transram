package com.obsidiandynamics.transram;

public final class Size implements DeepCloneable<Size> {
  private int value;

  public Size(int value) {
    this.value = value;
  }

  public int get() {
    return value;
  }

  public void set(int value) {
    this.value = value;
  }

  @Override
  public Size deepClone() {
    return new Size(value);
  }

  public String toString() {
    return Size.class.getSimpleName() + '[' + value + ']';
  }
}
