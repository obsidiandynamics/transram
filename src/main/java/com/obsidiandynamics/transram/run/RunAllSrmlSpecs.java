package com.obsidiandynamics.transram.run;

public class RunAllSrmlSpecs {
  public static void main(String[] args) throws InterruptedException {
    RunSrmlBankSpec.main(args);
    RunSrmlHighlanderSpec.main(args);
    RunSrmlHospitalSpec.main(args);
  }
}
