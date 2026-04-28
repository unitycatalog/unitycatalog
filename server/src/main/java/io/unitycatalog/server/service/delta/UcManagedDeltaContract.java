package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeatureKind;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
