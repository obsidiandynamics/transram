package com.obsidiandynamics.transram;

public final class BrokenSnapshotFailure extends ConcurrentModeFailure {
  public BrokenSnapshotFailure(String m) {
    super(m, null);
  }
}
