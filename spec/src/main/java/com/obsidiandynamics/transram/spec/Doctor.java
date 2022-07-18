package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;

public final class Doctor implements DeepCloneable<Doctor> {
  private boolean rostered;

  public Doctor(boolean rostered) {
    this.rostered = rostered;
  }

  public boolean isRostered() {
    return rostered;
  }

  public void setRostered(boolean rostered) {
    this.rostered = rostered;
  }

  @Override
  public String toString() {
    return Doctor.class.getSimpleName() + "[rostered=" + rostered + ']';
  }

  @Override
  public Doctor deepClone() {
    return new Doctor(rostered);
  }
}
