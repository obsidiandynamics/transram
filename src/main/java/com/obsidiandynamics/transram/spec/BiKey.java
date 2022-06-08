package com.obsidiandynamics.transram.spec;

public final class BiKey {
  private final int prefix;
  private final int suffix;

  public BiKey(int prefix, int suffix) {
    this.prefix = prefix;
    this.suffix = suffix;
  }

  public int getPrefix() {
    return prefix;
  }

  public int getSuffix() {
    return suffix;
  }

  @Override
  public int hashCode() {
    return (31 * 7 + prefix) * 7 + suffix;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o instanceof BiKey) {
      final var that = (BiKey) o;
      return prefix == that.prefix && suffix == that.suffix;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return BiKey.class.getSimpleName() + '[' + prefix + '_' + suffix + ']';
  }
}
