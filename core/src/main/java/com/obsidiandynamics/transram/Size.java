package com.obsidiandynamics.transram;

final class Size implements DeepCloneable<Size> {
  private int value;

  Size(int value) {
    this.value = value;
  }

  int get() {
    return value;
  }

  void set(int value) {
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
