package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.mutex.*;

public class RunSrml2Workload {
  public static void main(String[] args) throws InterruptedException {
    Harness.run(() -> new Srml2Map<>(new Srml2Map.Options() {{
      mutexStripes = 1024;
      mutexFactory = UnfairUpgradeableMutex::new;
    }}));
  }
}
