package com.obsidiandynamics.transram;

public final class LifecycleFailure extends ConcurrentModeFailure {
  public enum Reason {
    INSERT_EXISTING,
    UPDATE_NONEXISTENT,
    INSERT_DELETE_EXISTING,
    DELETE_NONEXISTENT
  }

  private final Reason reason;

  public LifecycleFailure(Reason reason, String m) {
    super(m, null);
    this.reason = reason;
  }

  public Reason getReason() {
    return reason;
  }
}
