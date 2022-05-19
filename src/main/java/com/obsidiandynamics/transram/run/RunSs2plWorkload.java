package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.mutex.*;

public class RunSs2plWorkload {
  public static void main(String[] args) throws InterruptedException {
    Harness.run(() -> new Ss2plMap<>(new Ss2plMap.Options() {{
      mutexStripes = 1024;
      mutexFactory = UnfairUpgradeableMutex::new;
      mutexTimeoutMs = 0;
    }}));
  }
}
