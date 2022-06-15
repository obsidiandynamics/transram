package com.obsidiandynamics.transram;

public final class IllegalLifecycleStateException extends IllegalStateException {
  public enum Reason {
    INSERT_EXISTING,
    UPDATE_NONEXISTENT,
    DELETE_NONEXISTENT,
    NEGATIVE_SIZE
  }

  private final Reason reason;

  public IllegalLifecycleStateException(Reason reason, String m) {
    super(m, null);
    this.reason = reason;
  }

  public Reason getReason() {
    return reason;
  }
}
