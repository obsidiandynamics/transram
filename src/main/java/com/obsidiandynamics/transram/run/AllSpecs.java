package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.spec.*;

import java.util.*;

final class AllSpecs {
  @SuppressWarnings({"unchecked", "rawtypes"})
  static void run(MapFactory mapFactory) throws InterruptedException {
    final var specs = Set.of(new BankSpec(), new HighlanderSpec(), new HospitalSpec());
    for (var spec : specs) {
      Harness.run(mapFactory, (Spec) spec);
    }
  }
}
