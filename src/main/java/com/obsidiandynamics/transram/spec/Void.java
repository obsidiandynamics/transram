package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;

public final class Void implements DeepCloneable<Void> {
  private static final Void INSTANCE = new Void();

  private Void() {}

  public static Void instance() { return INSTANCE; }

  @Override
  public Void deepClone() {
    return INSTANCE;
  }

  @Override
  public String toString() {
    return Void.class.getSimpleName();
  }
}
