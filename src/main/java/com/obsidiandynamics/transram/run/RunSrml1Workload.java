package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.mutex.*;

public class RunSrml1Workload {
  public static void main(String[] args) throws InterruptedException {
    Harness.run(() -> new Srml1Map<>(new Srml1Map.Options() {{
      mutexStripes = 1024;
      mutexFactory = UnfairUpgradeableMutex::new;
    }}));
  }
}
