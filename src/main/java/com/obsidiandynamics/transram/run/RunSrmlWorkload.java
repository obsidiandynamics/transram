package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.mutex.*;

public class RunSrmlWorkload {
  public static void main(String[] args) throws InterruptedException {
    Harness.run(() -> new SrmlMap<>(new SrmlMap.Options() {{
      mutexStripes = 1024;
      mutexFactory = UnfairUpgradeableMutex::new;
      queueDepth = 4;
    }}));
  }
}
