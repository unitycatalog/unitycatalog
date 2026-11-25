package io.unitycatalog.server.persist;

import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.persist.dao.TableInfoDAO;

/** Managed entity types. Registered tables retain an existing directory instead of an ID path. */
public enum ManagedResourceType {
  TABLE("tables"),
  REGISTERED_TABLE("tables"),
  VOLUME("volumes"),
  REGISTERED_MODEL("models"),
  MODEL_VERSION("versions"),
  STAGING_TABLE("tables");

  private final String pathSegment;

  ManagedResourceType(String pathSegment) {
    this.pathSegment = pathSegment;
  }

  /** Returns the allocation segment; adopted directories need not contain it or the resource id. */
  public String pathSegment() {
    return pathSegment;
  }

  /** Cleanup kind for a managed table, whose in-place Iceberg directory may not carry its UC id. */
  public static ManagedResourceType forTable(TableInfoDAO table) {
    return DataSourceFormat.ICEBERG.getValue().equals(table.getDataSourceFormat())
            && !table.getUrl().endsWith("/" + TABLE.pathSegment() + "/" + table.getId())
        ? REGISTERED_TABLE
        : TABLE;
  }
}
