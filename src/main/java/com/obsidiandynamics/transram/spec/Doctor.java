package com.obsidiandynamics.transram.spec;

public final class Doctor {
  private boolean onHoliday;

  public boolean isOnHoliday() {
    return onHoliday;
  }

  public void setOnHoliday(boolean onHoliday) {
    this.onHoliday = onHoliday;
  }

  public String toString() {
    return Doctor.class.getSimpleName() + "[onHoliday=" + onHoliday + ']';
  }
}
