package io.unitycatalog.hadoop.internal;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Objects;

/**
 * Internal identifier for a UC Delta table by catalog, schema, and table name.
 *
 * <p>This identifier is used when building Hadoop configuration properties for the UC Delta
 * temporary credentials API.
 */
public final class UCDeltaTableIdentifier {
  private final String catalog;
  private final String schema;
  private final String table;

  private UCDeltaTableIdentifier(String catalog, String schema, String table) {
    Preconditions.checkArgument(catalog != null && !catalog.isEmpty(), "catalog is required");
    Preconditions.checkArgument(schema != null && !schema.isEmpty(), "schema is required");
    Preconditions.checkArgument(table != null && !table.isEmpty(), "table is required");
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
  }

  /** Creates an identifier for the table {@code catalog.schema.table}. */
  public static UCDeltaTableIdentifier of(String catalog, String schema, String table) {
    return new UCDeltaTableIdentifier(catalog, schema, table);
  }

  /** Returns the UC catalog name. */
  public String catalog() {
    return catalog;
  }

  /** Returns the UC schema name. */
  public String schema() {
    return schema;
  }

  /** Returns the UC table name. */
  public String table() {
    return table;
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalog, schema, table);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof UCDeltaTableIdentifier)) {
      return false;
    } else if (this == o) {
      return true;
    }
    UCDeltaTableIdentifier that = (UCDeltaTableIdentifier) o;
    return Objects.equals(catalog, that.catalog)
        && Objects.equals(schema, that.schema)
        && Objects.equals(table, that.table);
  }

  @Override
  public String toString() {
    return String.format("%s.%s.%s", catalog, schema, table);
  }
}
