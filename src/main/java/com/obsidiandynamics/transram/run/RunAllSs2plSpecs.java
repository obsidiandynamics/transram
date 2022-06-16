package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.mutex.*;

public class RunAllSs2plSpecs {
  public static void main(String[] args) throws InterruptedException {
    AllSpecs.run(Ss2plMap.factory(new Ss2plMap.Options() {{
      mutexStripes = 1024;
      mutexFactory = UnfairUpgradeableMutex::new;
      mutexTimeoutMs = 0;
    }}));
  }
}
