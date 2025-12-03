package io.unitycatalog.spark;

public class UCTableProperties {
  private UCTableProperties() {
  }

  // This table property should be set to the table ID assigned by UC for managed tables.
  // It used to be "ucTableId". The old property is also set while the property is being renamed.
  public static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";
  public static final String UC_TABLE_ID_KEY_OLD = "ucTableId";

  // Prefix of table feature property key.
  public static final String FEATURE_PROP_PREFIX = "delta.feature.";

  // This table property should be set in order to enable Delta code to use UC as commit coordinator
  public static final String CATALOG_MANAGED_KEY = "delta.feature.catalogOwned-preview";
  public static final String CATALOG_MANAGED_VALUE = "supported";
  // Eventually Delta will be changed to use this feature name instead. But before that is done, we
  // can't set it yet.
  public static final String CATALOG_MANAGED_KEY_NEW = "delta.feature.catalogManaged";
}
