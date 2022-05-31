package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.spec.*;

public class RunSrml2Workload {
  public static void main(String[] args) throws InterruptedException {
    SpecHarness.run(Srml2Map.factory(new Srml2Map.Options() {{
      mutexStripes = 1024;
      mutexFactory = UnfairUpgradeableMutex::new;
    }}), new BankSpec());
  }
}
