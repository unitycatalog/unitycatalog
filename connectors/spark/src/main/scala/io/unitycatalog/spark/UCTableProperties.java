package io.unitycatalog.spark;

import org.apache.spark.sql.connector.catalog.TableCatalog;

import java.util.Set;

public class UCTableProperties {
  private UCTableProperties() {}

  // This table property should be set to the table ID assigned by UC for managed tables.
  // It used to be "ucTableId". The old property is also set while the property is being renamed.
  public static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";
  public static final String UC_TABLE_ID_KEY_OLD = "ucTableId";

  // This table property should be set in order to enable Delta code to use UC as commit coordinator
  public static final String DELTA_CATALOG_MANAGED_KEY = "delta.feature.catalogOwned-preview";
  public static final String DELTA_CATALOG_MANAGED_VALUE = "supported";
  // Eventually Delta will be changed to use this feature name instead. But before that is done, we
  // can't set it yet.
  public static final String DELTA_CATALOG_MANAGED_KEY_NEW = "delta.feature.catalogManaged";

  // These properties were added by `V1Table.addV2TableProperties` in package spark-catalyst.
  // They are used for constructing the CreateTable rpc.
  public static final Set<String> V2_TABLE_PROPERTIES =
      Set.of(
          TableCatalog.PROP_COMMENT,
          TableCatalog.PROP_EXTERNAL,
          TableCatalog.PROP_IS_MANAGED_LOCATION,
          TableCatalog.PROP_LOCATION,
          TableCatalog.PROP_OWNER,
          TableCatalog.PROP_PROVIDER);
}
