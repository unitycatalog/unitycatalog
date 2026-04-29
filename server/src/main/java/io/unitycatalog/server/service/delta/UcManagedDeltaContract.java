package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DomainMetadataUpdates;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeatureKind;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The UC catalog-managed Delta table contract: protocol versions, table features, and table
 * properties that any UC catalog-managed Delta table must carry. {@link DeltaStagingTableMapper}
 * advertises this contract in the staging response; future create / update / commit endpoints will
 * validate incoming requests against the same constants, so producer (advertise) and consumer
 * (validate) cannot drift.
 */
public final class UcManagedDeltaContract {

  private UcManagedDeltaContract() {}

  /** Reader version 3 is the minimum that supports per-feature reader-features negotiation. */
  public static final int REQUIRED_MIN_READER_VERSION = 3;

  /** Writer version 7 is the minimum that supports per-feature writer-features negotiation. */
  public static final int REQUIRED_MIN_WRITER_VERSION = 7;

  /**
   * Required features for a UC catalog-managed Delta table. Reader-features vs writer-features wire
   * lists are derived once from each feature's {@link TableFeatureKind}; only the typed list is
   * maintained, the four wire lists follow.
   */
  public static final List<TableFeature> REQUIRED_FEATURES =
      List.of(
          TableFeature.CATALOG_MANAGED,
          TableFeature.DELETION_VECTORS,
          TableFeature.V2_CHECKPOINT,
          TableFeature.VACUUM_PROTOCOL_CHECK,
          TableFeature.IN_COMMIT_TIMESTAMP);

  /**
   * Suggested features. {@code domainMetadata} is conditionally required with {@code rowTracking}
   * but is advertised as suggested so clients that enable row tracking enable it too.
   */
  public static final List<TableFeature> SUGGESTED_FEATURES =
      List.of(TableFeature.COLUMN_MAPPING, TableFeature.DOMAIN_METADATA, TableFeature.ROW_TRACKING);

  // Wire-format spec-name lists, computed once from the typed feature lists above.
  public static final List<String> REQUIRED_READER_FEATURES = readerFeaturesOf(REQUIRED_FEATURES);
  public static final List<String> REQUIRED_WRITER_FEATURES = writerFeaturesOf(REQUIRED_FEATURES);
  public static final List<String> SUGGESTED_READER_FEATURES = readerFeaturesOf(SUGGESTED_FEATURES);
  public static final List<String> SUGGESTED_WRITER_FEATURES = writerFeaturesOf(SUGGESTED_FEATURES);

  /**
   * Required properties with fixed values. {@code io.unitycatalog.tableId} is filled in per-request
   * from the UC-allocated table UUID and is checked separately by the consumer.
   */
  public static final Map<String, String> REQUIRED_FIXED_PROPERTIES =
      Map.of(
          TableProperties.CHECKPOINT_POLICY, "v2",
          TableProperties.ENABLE_DELETION_VECTORS, "true",
          TableProperties.ENABLE_IN_COMMIT_TIMESTAMPS, "true");

  /**
   * Required properties whose value the engine must compute at commit time. The staging response
   * sends them with {@code null} values; the createTable request must echo them back with non-null
   * values the engine substituted.
   */
  public static final List<String> ENGINE_GENERATED_PROPERTY_KEYS =
      List.of(
          TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
          TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP);

  /**
   * Suggested properties. Null values mean the client generates the value at commit time (required
   * when the corresponding feature is enabled). Built via a helper because {@link Map#of} rejects
   * null values; wrapped unmodifiable so the same instance can be shared across all responses.
   */
  public static final Map<String, String> SUGGESTED_PROPERTIES = buildSuggestedProperties();

  private static Map<String, String> buildSuggestedProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(TableProperties.ENABLE_ROW_TRACKING, "true");
    props.put(TableProperties.ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_NAME, null);
    props.put(TableProperties.ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME, null);
    return Collections.unmodifiableMap(props);
  }

  /**
   * Validates that the supplied protocol, domain-metadata, and properties satisfy the contract:
   *
   * <ul>
   *   <li>the protocol's reader-features are a subset of its writer-features (Delta-spec rule);
   *   <li>protocol versions meet the required minimums;
   *   <li>every required feature is present in the appropriate wire list;
   *   <li>every declared domain-metadata entry is backed by the matching writer feature;
   *   <li>every fixed-value required property equals the expected value;
   *   <li>the UC table-id property is set;
   *   <li>the engine-generated required properties are present with non-null values.
   * </ul>
   *
   * <p>Apply only when the table is UC catalog-managed (i.e. MANAGED). EXTERNAL tables don't go
   * through staging and don't carry this contract.
   *
   * <p>Designed to be called from {@link DeltaCreateTableMapper}, future update / commit mappers,
   * and anywhere else that needs to assert "this state is a valid UC-managed Delta table state."
   */
  public static void validate(
      DeltaProtocol protocol,
      DomainMetadataUpdates domainMetadata,
      Map<String, String> properties) {
    Objects.requireNonNull(protocol, "protocol");
    validateReaderFeatureSubset(protocol);
    validateRequiredVersions(protocol);
    validateRequiredFeatures(protocol);
    validateDomainMetadataAgainstProtocol(protocol, domainMetadata);
    validateRequiredProperties(properties != null ? properties : Map.of());
  }

  /**
   * Pins the Delta-spec invariant that every entry in {@code reader-features} must also be in
   * {@code writer-features}. Reader-writer features appear in both lists; writer-only features
   * appear only in writer-features; there is no reader-only feature. A request that places a
   * feature in {@code reader-features} but not {@code writer-features} would yield a stored
   * protocol no Delta writer can honor.
   */
  private static void validateReaderFeatureSubset(DeltaProtocol protocol) {
    List<String> readerFeatures = protocol.getReaderFeatures();
    if (readerFeatures == null || readerFeatures.isEmpty()) return;
    Set<String> writerFeatures =
        protocol.getWriterFeatures() != null ? Set.copyOf(protocol.getWriterFeatures()) : Set.of();
    for (String rf : readerFeatures) {
      if (!writerFeatures.contains(rf)) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Feature '"
                + rf
                + "' is in reader-features but not writer-features. Per the Delta protocol, every"
                + " reader-feature must also be a writer-feature.");
      }
    }
  }

  /**
   * Cross-check the client's claim against UC's own state: {@code properties[UC_TABLE_ID]} must
   * equal the UC-allocated UUID for the table being created or updated. The {@code expectedTableId}
   * is the source of truth -- the staging table's UUID at create time, the existing table's UUID at
   * update time. {@link #validate} only checks presence; this method checks identity.
   */
  public static void validateTableIdProperty(
      Map<String, String> properties, String expectedTableId) {
    Objects.requireNonNull(expectedTableId, "expectedTableId");
    String actual = properties != null ? properties.get(TableProperties.UC_TABLE_ID) : null;
    if (!expectedTableId.equals(actual)) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "Property %s (%s) does not match the table id (%s).",
              TableProperties.UC_TABLE_ID, actual, expectedTableId));
    }
  }

  private static void validateRequiredVersions(DeltaProtocol protocol) {
    Integer minReader = protocol.getMinReaderVersion();
    if (minReader == null || minReader < REQUIRED_MIN_READER_VERSION) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "MANAGED table minReaderVersion must be at least "
              + REQUIRED_MIN_READER_VERSION
              + " (got "
              + minReader
              + ").");
    }
    Integer minWriter = protocol.getMinWriterVersion();
    if (minWriter == null || minWriter < REQUIRED_MIN_WRITER_VERSION) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "MANAGED table minWriterVersion must be at least "
              + REQUIRED_MIN_WRITER_VERSION
              + " (got "
              + minWriter
              + ").");
    }
  }

  private static void validateRequiredFeatures(DeltaProtocol protocol) {
    Set<String> readerFeatures =
        Set.copyOf(protocol.getReaderFeatures() != null ? protocol.getReaderFeatures() : List.of());
    Set<String> writerFeatures =
        Set.copyOf(protocol.getWriterFeatures() != null ? protocol.getWriterFeatures() : List.of());
    for (String required : REQUIRED_READER_FEATURES) {
      if (!readerFeatures.contains(required)) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "MANAGED table missing required reader-feature '" + required + "'.");
      }
    }
    for (String required : REQUIRED_WRITER_FEATURES) {
      if (!writerFeatures.contains(required)) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "MANAGED table missing required writer-feature '" + required + "'.");
      }
    }
  }

  /**
   * Validates that each declared domain-metadata entry is backed by the matching writer feature in
   * the protocol. A {@code delta.clustering} entry requires the {@code clustering} feature; a
   * {@code delta.rowTracking} entry requires the {@code rowTracking} feature. Null domain-metadata
   * is permitted; null protocol is treated as "no writer features declared" so any non-null
   * domain-metadata fails.
   */
  private static void validateDomainMetadataAgainstProtocol(
      DeltaProtocol protocol, DomainMetadataUpdates domainMetadata) {
    if (domainMetadata == null) return;
    List<String> writerFeatures =
        protocol.getWriterFeatures() != null ? protocol.getWriterFeatures() : List.of();
    if (domainMetadata.getDeltaClustering() != null
        && !writerFeatures.contains(TableFeature.CLUSTERING.specName())) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "domain-metadata.delta.clustering requires the '"
              + TableFeature.CLUSTERING.specName()
              + "' writer feature.");
    }
    if (domainMetadata.getDeltaRowTracking() != null
        && !writerFeatures.contains(TableFeature.ROW_TRACKING.specName())) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "domain-metadata.delta.rowTracking requires the '"
              + TableFeature.ROW_TRACKING.specName()
              + "' writer feature.");
    }
  }

  private static void validateRequiredProperties(Map<String, String> properties) {
    for (Map.Entry<String, String> entry : REQUIRED_FIXED_PROPERTIES.entrySet()) {
      String actual = properties.get(entry.getKey());
      if (!entry.getValue().equals(actual)) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "MANAGED table required property '"
                + entry.getKey()
                + "' must be '"
                + entry.getValue()
                + "' (got '"
                + actual
                + "').");
      }
    }
    String tableId = properties.get(TableProperties.UC_TABLE_ID);
    if (tableId == null || tableId.isBlank()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "MANAGED table required property '" + TableProperties.UC_TABLE_ID + "' is missing.");
    }
    for (String key : ENGINE_GENERATED_PROPERTY_KEYS) {
      if (properties.get(key) == null) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "MANAGED table required property '" + key + "' must be set by the engine.");
      }
    }
  }

  /** Spec names of the {@link TableFeatureKind#READER_WRITER} subset, in declaration order. */
  private static List<String> readerFeaturesOf(List<TableFeature> features) {
    return features.stream()
        .filter(f -> f.kind() == TableFeatureKind.READER_WRITER)
        .map(TableFeature::specName)
        .toList();
  }

  /** Spec names of all features (reader-writer + writer-only), in declaration order. */
  private static List<String> writerFeaturesOf(List<TableFeature> features) {
    return features.stream().map(TableFeature::specName).toList();
  }
}
