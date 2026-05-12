package io.unitycatalog.hadoop;

/**
 * The access operation requested for a Unity Catalog table credential.
 *
 * <p>Shared across both the UC REST credentials API and the UC Delta credentials API.
 *
 * @since 0.5.0
 */
public enum TableOperation {
  READ("READ"),
  READ_WRITE("READ_WRITE");

  private final String value;

  TableOperation(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
