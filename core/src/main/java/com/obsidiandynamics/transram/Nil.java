package com.obsidiandynamics.transram;

public final class Nil implements DeepCloneable<Nil> {
  private static final Nil INSTANCE = new Nil();

  private Nil() {}

  public static Nil instance() { return INSTANCE; }

  @Override
  public Nil deepClone() {
    return INSTANCE;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(INSTANCE);
  }

  @Override
  public boolean equals(Object o) {
    //noinspection ConditionCoveredByFurtherCondition
    return o == this || o instanceof Nil;
  }

  @Override
  public String toString() {
    return Nil.class.getSimpleName();
  }
}
