package io.unitycatalog.server.persist;

/** Managed entity type and the storage path segment before its id: {@code .../<segment>/<id>}. */
public enum ManagedResourceType {
  TABLE("tables"),
  VOLUME("volumes"),
  REGISTERED_MODEL("models"),
  MODEL_VERSION("versions"),
  STAGING_TABLE("tables");

  private final String pathSegment;

  ManagedResourceType(String pathSegment) {
    this.pathSegment = pathSegment;
  }

  /** Returns the path segment immediately before the resource id. */
  public String pathSegment() {
    return pathSegment;
  }
}
