package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.spec.*;

public class RunSs2plWorkload {
  public static void main(String[] args) throws InterruptedException {
    SpecHarness.run(Ss2plMap.factory(new Ss2plMap.Options() {{
      mutexStripes = 1024;
      mutexFactory = UnfairUpgradeableMutex::new;
      mutexTimeoutMs = 0;
    }}), new BankSpec());
  }
}
