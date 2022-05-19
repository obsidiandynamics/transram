package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.Enclose.Region.*;
import com.obsidiandynamics.transram.lock.*;
import com.obsidiandynamics.transram.util.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class RunSs2plWorkload {
  public static void main(String[] args) throws InterruptedException {
    Harness.run(() -> new Ss2plMap<>(new Ss2plMap.Options() {{
      lockStripes = 1024;
      lockFactory = UnfairUpgradeableLock::new;
      lockTimeoutMs = 0;
    }}));
  }
}
