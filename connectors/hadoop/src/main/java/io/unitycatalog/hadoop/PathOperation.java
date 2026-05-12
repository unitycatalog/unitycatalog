package io.unitycatalog.hadoop;

/**
 * The access operation requested for a Unity Catalog external path credential.
 *
 * @since 0.5.0
 */
public enum PathOperation {
  PATH_READ("PATH_READ"),
  PATH_READ_WRITE("PATH_READ_WRITE"),
  PATH_CREATE_TABLE("PATH_CREATE_TABLE");

  private final String value;

  PathOperation(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
