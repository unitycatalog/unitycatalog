package io.unitycatalog.server.utils;

public class TableProperties {
  /** Key for identifying Unity Catalog table ID in Delta commits. */
  public static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";

  /** Last metadata-changing commit version (delta.lastUpdateVersion). */
  public static final String LAST_UPDATE_VERSION = "delta.lastUpdateVersion";

  /** Timestamp of the last metadata-changing commit (delta.lastCommitTimestamp). */
  public static final String LAST_COMMIT_TIMESTAMP = "delta.lastCommitTimestamp";
}
