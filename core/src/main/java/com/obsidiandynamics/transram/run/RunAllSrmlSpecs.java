package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.mutex.*;

public class RunAllSrmlSpecs {
  public static void main(String[] args) throws InterruptedException {
    AllSpecs.run(SrmlMap.factory(new SrmlMap.Options() {{
      mutexStripes = 1024;
      mutexFactory = UnfairUpgradeableMutex::new;
      queueDepth = 4;
    }}));
  }
}
