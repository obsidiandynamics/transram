package com.obsidiandynamics.transram;

public final class MutexAcquisitionFailure extends ConcurrentModeFailure {
  public MutexAcquisitionFailure(String m, Throwable cause) {
    super(m, cause);
  }
}
