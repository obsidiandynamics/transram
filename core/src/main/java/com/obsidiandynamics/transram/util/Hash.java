package com.obsidiandynamics.transram.util;

import java.util.*;

public final class Hash {
  public static int byModulo(Object obj, int modulo) {
    final var rawModHash = Objects.hashCode(obj) % modulo;
    return rawModHash < 0 ? rawModHash + modulo : rawModHash;
  }
}
