package io.unitycatalog.server.service.delta;

/**
 * Spec-defined Delta protocol identifiers referenced by the Delta REST Catalog surface. Centralised
 * here so that feature-specific server decisions (e.g. "MANAGED tables must declare {@code
 * catalogManaged}") and required-property declarations cannot silently mismatch the strings the
 * client actually writes into the Delta log.
 */
public final class DeltaConsts {

  private DeltaConsts() {}

  /**
   * Reader/writer classification for a {@link TableFeature}, per the Delta protocol spec.
   *
   * <ul>
   *   <li>{@link #READER_WRITER}: feature must appear in BOTH {@code protocol.reader-features} and
   *       {@code protocol.writer-features}.
   *   <li>{@link #WRITER_ONLY}: feature appears only in {@code protocol.writer-features}.
   * </ul>
   *
   * Delta has no reader-only features.
   */
  public enum TableFeatureKind {
    READER_WRITER,
    WRITER_ONLY
  }

  /**
   * Table-feature identifiers as they appear in {@code protocol.reader-features} and {@code
   * protocol.writer-features}. Each entry's spec name and reader/writer classification match
   * delta-io's {@code TableFeature.scala} subclass hierarchy.
   */
  public enum TableFeature {
    CATALOG_MANAGED("catalogManaged", TableFeatureKind.READER_WRITER),
    DELETION_VECTORS("deletionVectors", TableFeatureKind.READER_WRITER),
    V2_CHECKPOINT("v2Checkpoint", TableFeatureKind.READER_WRITER),
    VACUUM_PROTOCOL_CHECK("vacuumProtocolCheck", TableFeatureKind.READER_WRITER),
    COLUMN_MAPPING("columnMapping", TableFeatureKind.READER_WRITER),
    IN_COMMIT_TIMESTAMP("inCommitTimestamp", TableFeatureKind.WRITER_ONLY),
    DOMAIN_METADATA("domainMetadata", TableFeatureKind.WRITER_ONLY),
    ROW_TRACKING("rowTracking", TableFeatureKind.WRITER_ONLY),
    CLUSTERING("clustering", TableFeatureKind.WRITER_ONLY);

    private final String specName;
    private final TableFeatureKind kind;

    TableFeature(String specName, TableFeatureKind kind) {
      this.specName = specName;
      this.kind = kind;
    }

    /** The spec-defined wire string (e.g. {@code "catalogManaged"}). */
    public String specName() {
      return specName;
    }

    public TableFeatureKind kind() {
      return kind;
    }
  }

  /**
   * Table-property keys referenced by the Delta REST Catalog surface. The {@code delta.*} keys are
   * defined by the Delta protocol; {@link #UC_TABLE_ID} is the UC-namespace rule-based property
   * that binds a Delta table to its UC-allocated UUID.
   *
   * <p>Named {@code TableProperties} (not {@code Properties}) so that, once imported standalone, it
   * does not collide with {@link java.util.Properties}.
   */
  public static final class TableProperties {
    private TableProperties() {}

    /** UC-namespace rule-based property binding a Delta table to its UC-allocated UUID. */
    public static final String UC_TABLE_ID = "io.unitycatalog.tableId";

    /** Last metadata-changing commit version. */
    public static final String LAST_UPDATE_VERSION = "delta.lastUpdateVersion";

    /** Timestamp of the last metadata-changing commit. */
    public static final String LAST_COMMIT_TIMESTAMP = "delta.lastCommitTimestamp";

    public static final String CHECKPOINT_POLICY = "delta.checkpointPolicy";
    public static final String CHECKPOINT_WRITE_STATS_AS_JSON = "delta.checkpoint.writeStatsAsJson";
    public static final String CHECKPOINT_WRITE_STATS_AS_STRUCT =
        "delta.checkpoint.writeStatsAsStruct";
    public static final String ENABLE_DELETION_VECTORS = "delta.enableDeletionVectors";
    public static final String ENABLE_IN_COMMIT_TIMESTAMPS = "delta.enableInCommitTimestamps";
    public static final String IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION =
        "delta.inCommitTimestampEnablementVersion";
    public static final String IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP =
        "delta.inCommitTimestampEnablementTimestamp";
    public static final String ENABLE_ROW_TRACKING = "delta.enableRowTracking";
    public static final String ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_NAME =
        "delta.rowTracking.materializedRowIdColumnName";
    public static final String ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME =
        "delta.rowTracking.materializedRowCommitVersionColumnName";
  }
}
