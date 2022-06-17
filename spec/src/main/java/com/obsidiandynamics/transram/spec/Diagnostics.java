package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;

final class Diagnostics {
  static void dumpMap(TransMap<?, ?> map) {
    for (var entry : map.debug().dirtyView().entrySet()) {
      System.out.format("%10s:%s\n", entry.getKey(), entry.getValue());
    }
  }
}
