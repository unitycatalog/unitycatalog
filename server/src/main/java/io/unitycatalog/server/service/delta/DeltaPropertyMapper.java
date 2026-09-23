package io.unitycatalog.server.service.delta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.delta.model.DeltaClusteringDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.server.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DeltaRowTrackingDomainMetadata;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.delta.DeltaConsts.DomainMetadataNames;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Assembles the stored UC table-property map from a {@link DeltaCreateTableRequest} and the
 * per-block helpers ({@link #deriveFromProtocol} / {@link #deriveFromDomainMetadata}). Designed to
 * be shared by create, update, and commit endpoints: the mapping rules are specified by the Delta
 * spec and don't vary by endpoint (only create is wired up today; update and commit are on
 * follow-ups).
 *
 * <p>Every feature in {@code protocol.reader-features} and {@code protocol.writer-features} yields
 * {@code delta.feature.<name> = supported}. Reader-writer features (which appear in both lists)
 * collapse to a single entry via map dedup.
 *
 * <p>Domain-metadata entries yield their Delta-documented table-property projections:
 *
 * <ul>
 *   <li>The {@code delta.clustering} domain-metadata entry's {@code clusteringColumns} field
 *       projects to the {@code delta.clusteringColumns} table property as a JSON-encoded list of
 *       column paths. Paths are arrays of segment names so nested columns survive as arrays rather
 *       than collapsing into dotted strings.
 *   <li>The {@code delta.rowTracking} domain-metadata entry's {@code rowIdHighWaterMark} field
 *       projects to the {@code delta.rowTracking.rowIdHighWaterMark} table property.
 * </ul>
 *
 * <p>Consistency rules (e.g. domain-metadata entry implies its writer feature) live in {@link
 * UcManagedDeltaContract}, not here.
 */
public final class DeltaPropertyMapper {

  private DeltaPropertyMapper() {}

  private static final ObjectMapper JSON = new ObjectMapper();

  /** Value emitted under {@code delta.feature.<name>} for each declared feature. */
  private static final String FEATURE_SUPPORTED = "supported";

  /**
   * Builds the stored UC property map by layering client-supplied entries first, then overlaying
   * protocol- and domain-metadata-derived entries on top. The structured {@code protocol} and
   * {@code domain-metadata} blocks are the canonical source for the keys this method derives
   * ({@code delta.feature.*}, {@code delta.clusteringColumns}, {@code
   * delta.rowTracking.rowIdHighWaterMark}); a stray client property under those names cannot
   * silently override the projection. Client-only properties (e.g. {@code
   * delta.rowTracking.materializedRowIdColumnName}) flow through untouched.
   */
  public static Map<String, String> buildStoredProperties(DeltaCreateTableRequest req) {
    Map<String, String> merged = new HashMap<>();
    if (req.getProperties() != null) {
      merged.putAll(req.getProperties());
    }
    deriveFromProtocol(merged, req.getProtocol());
    deriveFromDomainMetadata(merged, req.getDomainMetadata());
    if (req.getLastCommitTimestampMs() != null) {
      merged.put(
          TableProperties.LAST_COMMIT_TIMESTAMP, String.valueOf(req.getLastCommitTimestampMs()));
    }
    // For createTable it's always the 0 commit
    merged.put(TableProperties.LAST_UPDATE_VERSION, "0");
    return merged;
  }

  /**
   * Adds the {@code delta.feature.*} properties implied by a protocol block into {@code props}
   * in-place. Iterates both reader- and writer-features; reader-writer features (which appear in
   * both lists) collapse to a single entry via map dedup. The mapper does not assume the protocol
   * has been validated -- if a reader-feature is missing from writer-features (a Delta-spec
   * violation that {@link UcManagedDeltaContract#validate} catches), this method still emits an
   * entry for it.
   */
  public static void deriveFromProtocol(Map<String, String> props, DeltaProtocol protocol) {
    if (protocol == null) return;
    props.put(TableProperties.MIN_READER_VERSION, protocol.getMinReaderVersion().toString());
    props.put(TableProperties.MIN_WRITER_VERSION, protocol.getMinWriterVersion().toString());
    addFeatureProperties(props, protocol.getReaderFeatures());
    addFeatureProperties(props, protocol.getWriterFeatures());
  }

  /**
   * Domain-name -> stored-table-property-key projection, shared by the set and remove sides so they
   * cannot drift.
   */
  public static final Map<String, String> DOMAIN_TO_PROPERTY_KEY =
      Map.of(
          DomainMetadataNames.CLUSTERING, TableProperties.CLUSTERING_COLUMNS,
          DomainMetadataNames.ROW_TRACKING, TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK);

  /** Adds the table properties implied by a domain-metadata block into {@code props} in-place. */
  public static void deriveFromDomainMetadata(
      Map<String, String> props, DeltaDomainMetadataUpdates domainMetadata) {
    if (domainMetadata == null) return;
    DeltaClusteringDomainMetadata clustering = domainMetadata.getDeltaClustering();
    if (clustering != null && clustering.getClusteringColumns() != null) {
      // Use a proper JSON encoder rather than string-joining so nested column paths don't
      // collapse into dotted strings.
      props.put(
          DOMAIN_TO_PROPERTY_KEY.get(DomainMetadataNames.CLUSTERING),
          toJson(clustering.getClusteringColumns()));
    }
    DeltaRowTrackingDomainMetadata rowTracking = domainMetadata.getDeltaRowTracking();
    if (rowTracking != null && rowTracking.getRowIdHighWaterMark() != null) {
      props.put(
          DOMAIN_TO_PROPERTY_KEY.get(DomainMetadataNames.ROW_TRACKING),
          String.valueOf(rowTracking.getRowIdHighWaterMark()));
    }
  }

  /**
   * Inverse of {@link #deriveFromDomainMetadata}: reconstruct the effective {@link
   * DeltaDomainMetadataUpdates} from currently-stored UC properties, so the update path can
   * validate against entries still projected from those properties.
   */
  public static DeltaDomainMetadataUpdates synthesizeDomainMetadataFromProperties(
      Map<String, String> props) {
    DeltaDomainMetadataUpdates dm = new DeltaDomainMetadataUpdates();
    String clusteringJson = props.get(TableProperties.CLUSTERING_COLUMNS);
    if (clusteringJson != null) {
      try {
        List<List<String>> cols =
            JSON.readValue(clusteringJson, new TypeReference<List<List<String>>>() {});
        dm.deltaClustering(new DeltaClusteringDomainMetadata().clusteringColumns(cols));
      } catch (JsonProcessingException e) {
        throw new BaseException(
            ErrorCode.INTERNAL,
            String.format(
                "Corrupt stored property %s: %s",
                TableProperties.CLUSTERING_COLUMNS, e.getMessage()));
      }
    }
    String highWaterMark = props.get(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK);
    if (highWaterMark != null) {
      try {
        dm.deltaRowTracking(
            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(Long.parseLong(highWaterMark)));
      } catch (NumberFormatException e) {
        throw new BaseException(
            ErrorCode.INTERNAL,
            String.format(
                "Corrupt stored property %s: %s",
                TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK, e.getMessage()));
      }
    }
    return dm;
  }

  /**
   * Reads the {@code delta.feature.*} keys back out as a feature set. Safe to treat as
   * writer-features since the Delta spec makes every reader-feature also a writer-feature.
   */
  public static Set<String> extractFeaturesFromProperties(Map<String, String> props) {
    Set<String> features = new HashSet<>();
    for (String key : props.keySet()) {
      if (key.startsWith(TableProperties.FEATURE_PREFIX)) {
        features.add(key.substring(TableProperties.FEATURE_PREFIX.length()));
      }
    }
    return features;
  }

  private static void addFeatureProperties(Map<String, String> props, List<String> features) {
    if (features == null) return;
    for (String feature : features) {
      props.put(TableProperties.FEATURE_PREFIX + feature, FEATURE_SUPPORTED);
    }
  }

  private static String toJson(Object value) {
    try {
      return JSON.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new BaseException(
          ErrorCode.INTERNAL, "Failed to encode domain-metadata as JSON: " + e.getMessage());
    }
  }
}
