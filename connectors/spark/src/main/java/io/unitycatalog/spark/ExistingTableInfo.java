package io.unitycatalog.spark;

import java.util.Map;

/** Carrier for existing table metadata needed during REPLACE TABLE flows. */
public class ExistingTableInfo {
  private final String storageLocation;
  private final String tableId;
  private final boolean catalogManagedDelta;
  private final String dataSourceFormat;
  private final Map<String, String> properties;

  public ExistingTableInfo(
      String storageLocation,
      String tableId,
      boolean catalogManagedDelta,
      String dataSourceFormat,
      Map<String, String> properties) {
    this.storageLocation = storageLocation;
    this.tableId = tableId;
    this.catalogManagedDelta = catalogManagedDelta;
    this.dataSourceFormat = dataSourceFormat;
    this.properties = properties;
  }

  public String getStorageLocation() {
    return storageLocation;
  }

  public String getTableId() {
    return tableId;
  }

  public boolean isCatalogManagedDelta() {
    return catalogManagedDelta;
  }

  public String getDataSourceFormat() {
    return dataSourceFormat;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
