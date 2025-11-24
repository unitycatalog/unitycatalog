package io.unitycatalog.spark;

public class UCTableProperties {
  private UCTableProperties() {
  }

  // This table property should be set to the table ID assigned by UC for managed tables
  public static final String UC_TABLE_ID_KEY = "ucTableId";

  // This table property should be set in order to enable Delta code to use UC as commit coordinator
  public static final String CATALOG_MANAGED_KEY = "delta.feature.catalogOwned-preview";
  public static final String CATALOG_MANAGED_VALUE = "supported";
}
