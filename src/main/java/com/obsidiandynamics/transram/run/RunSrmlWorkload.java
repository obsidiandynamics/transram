package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.Enclose.Region.*;
import com.obsidiandynamics.transram.lock.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static com.obsidiandynamics.transram.util.Table.*;

public class RunSrmlWorkload {
  public static void main(String[] args) throws InterruptedException {
    Harness.run(() -> new SrmlMap<>(new SrmlMap.Options() {{
      lockStripes = 1024;
      lockFactory = UnfairUpgradeableLock::new;
    }}));
  }
}
