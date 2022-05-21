package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.mutex.*;

public class RunSrml3Workload {
  public static void main(String[] args) throws InterruptedException {
    Harness.run(() -> new Srml3Map<>(new Srml3Map.Options() {{
      mutexStripes = 1024;
      mutexFactory = UnfairUpgradeableMutex::new;
      queueDepth = 4;
    }}));
  }
}
