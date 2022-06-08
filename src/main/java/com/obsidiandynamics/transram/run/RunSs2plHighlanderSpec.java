package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.mutex.*;
import com.obsidiandynamics.transram.spec.*;

public class RunSs2plHighlanderSpec {
  public static void main(String[] args) throws InterruptedException {
    Harness.run(Ss2plMap.factory(new Ss2plMap.Options() {{
      mutexStripes = 1024;
      mutexFactory = UnfairUpgradeableMutex::new;
      mutexTimeoutMs = 0;
    }}), new HighlanderSpec());
  }
}
