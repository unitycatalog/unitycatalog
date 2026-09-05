package io.unitycatalog.server.persist.model;

/** Persistent states for deferred resource cleanup. */
public enum PurgeState {
  ACTIVE((short) 0),
  PENDING((short) 1),
  RUNNING((short) 2);

  private final short value;

  PurgeState(short value) {
    this.value = value;
  }

  public short getValue() {
    return value;
  }
}
