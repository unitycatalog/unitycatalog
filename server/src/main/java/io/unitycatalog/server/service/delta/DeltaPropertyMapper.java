package io.unitycatalog.server.service.delta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.delta.model.ClusteringDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DomainMetadataUpdates;
import io.unitycatalog.server.delta.model.RowTrackingDomainMetadata;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps Delta REST Catalog {@code protocol} and {@code domain-metadata} blocks onto UC table
 * properties. Designed to be shared by create, update, and commit endpoints: the mapping rules are
 * specified by the Delta spec and don't vary by endpoint (only create is wired up today; update and
 * commit are on follow-ups).
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
  public static Map<String, String> buildStoredProperties(
      DeltaProtocol protocol,
      DomainMetadataUpdates domainMetadata,
      Map<String, String> clientProperties) {
    Map<String, String> merged = new HashMap<>();
    if (clientProperties != null) {
      merged.putAll(clientProperties);
    }
    deriveFromProtocol(merged, protocol);
    deriveFromDomainMetadata(merged, domainMetadata);
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
    addFeatureProperties(props, protocol.getReaderFeatures());
    addFeatureProperties(props, protocol.getWriterFeatures());
  }

  /** Adds the table properties implied by a domain-metadata block into {@code props} in-place. */
  public static void deriveFromDomainMetadata(
      Map<String, String> props, DomainMetadataUpdates domainMetadata) {
    if (domainMetadata == null) return;
    ClusteringDomainMetadata clustering = domainMetadata.getDeltaClustering();
    if (clustering != null && clustering.getClusteringColumns() != null) {
      // Use a proper JSON encoder rather than string-joining so nested column paths don't
      // collapse into dotted strings.
      props.put(TableProperties.CLUSTERING_COLUMNS, toJson(clustering.getClusteringColumns()));
    }
    RowTrackingDomainMetadata rowTracking = domainMetadata.getDeltaRowTracking();
    if (rowTracking != null && rowTracking.getRowIdHighWaterMark() != null) {
      props.put(
          TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK,
          String.valueOf(rowTracking.getRowIdHighWaterMark()));
    }
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
