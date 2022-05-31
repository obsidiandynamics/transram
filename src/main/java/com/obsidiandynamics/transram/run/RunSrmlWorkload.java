package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.spec.*;

public class RunSrmlWorkload {
  public static void main(String[] args) throws InterruptedException {
    SpecHarness.run(SrmlMap.factory(new SrmlMap.Options() {{
      mutexStripes = 1024;
      mutexFactory = UnfairUpgradeableMutex::new;
      queueDepth = 4;
    }}), new BankSpec());
  }
}
