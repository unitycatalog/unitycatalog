package io.unitycatalog.spark;

import java.util.Set;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class UCTableProperties {
  private UCTableProperties() {}

  // This table property should be set to the table ID assigned by UC for managed tables.
  public static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";

  // This table property should be set in order to enable Delta code to use UC as commit coordinator
  public static final String DELTA_CATALOG_MANAGED_KEY = "delta.feature.catalogOwned-preview";
  public static final String DELTA_CATALOG_MANAGED_VALUE = "supported";
  // Eventually Delta will be changed to use this feature name instead. But before that is done, we
  // can't set it yet.
  public static final String DELTA_CATALOG_MANAGED_KEY_NEW = "delta.feature.catalogManaged";

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
          TableCatalog.PROP_TABLE_TYPE);
}
