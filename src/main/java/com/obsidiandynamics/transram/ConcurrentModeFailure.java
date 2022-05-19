package com.obsidiandynamics.transram;

public abstract class ConcurrentModeFailure extends Exception {
  ConcurrentModeFailure(String m, Throwable cause) {
    super(m, cause);
  }
}
