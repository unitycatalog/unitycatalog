package io.unitycatalog.spark;

import java.util.Set;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class UCTableProperties {
  private UCTableProperties() {}

  // This table property should be set to the table ID assigned by UC for managed tables.
  public static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";

  // This table property should be set in order to enable Delta code to use UC as commit coordinator
  public static final String DELTA_CATALOG_MANAGED_KEY = "delta.feature.catalogManaged";
  public static final String DELTA_CATALOG_MANAGED_VALUE = "supported";

  // Spark 4.2 added the `PROP_TABLE_TYPE = "table_type"` constant on `TableCatalog`. We
  // mirror it here so the connector compiles against Spark 4.0/4.1 too (where the constant
  // does not exist on the Spark interface). Value must stay in sync with Spark's.
  public static final String PROP_TABLE_TYPE = "table_type";

  // Reserved V2 table properties that are promoted to first-class fields on the UC `CreateTable`
  // payload (provider, location, comment, table_type, ...) and therefore must not
  // be forwarded to the server as part of the `properties` map -- otherwise they would be
  // double-persisted and would not round-trip cleanly on `loadTable`.
  //
  // View-specific fields such as query text and current catalog/namespace are typed fields on
  // Spark's `ViewInfo`, not TableCatalog properties.
  public static final Set<String> V2_TABLE_PROPERTIES =
      Set.of(
          TableCatalog.PROP_COMMENT,
          TableCatalog.PROP_COLLATION,
          TableCatalog.PROP_EXTERNAL,
          TableCatalog.PROP_IS_MANAGED_LOCATION,
          TableCatalog.PROP_LOCATION,
          TableCatalog.PROP_OWNER,
          TableCatalog.PROP_PROVIDER,
          PROP_TABLE_TYPE);
}
