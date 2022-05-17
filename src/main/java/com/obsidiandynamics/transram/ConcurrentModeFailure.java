package com.obsidiandynamics.transram;

public final class ConcurrentModeFailure extends Exception {
  public ConcurrentModeFailure(String m, Throwable cause) {
    super(m, cause);
  }
}
