package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.delta.model.DeltaClusteringDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.server.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DeltaRowTrackingDomainMetadata;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DeltaPropertyMapper}. This class is shared by create, update, and commit
 * endpoints, so its wire-format projections and consistency rules are pinned here directly rather
 * than only indirectly through one endpoint's integration tests.
 */
public class DeltaPropertyMapperTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  // ---------- deriveFromProtocol ----------

  @Test
  public void deriveFromProtocolProducesFeaturePropertiesFromWriterFeatures() {
    // Per the Delta spec, every reader-feature must also be a writer-feature, so the writer
    // list is the superset. The mapper iterates writer-features only and that's sufficient to
    // emit a delta.feature.<name> entry for every spec-defined feature.
    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(3)
            .minWriterVersion(7)
            .readerFeatures(
                List.of(
                    TableFeature.DELETION_VECTORS.specName(),
                    TableFeature.V2_CHECKPOINT.specName()))
            .writerFeatures(
                List.of(
                    TableFeature.CATALOG_MANAGED.specName(),
                    TableFeature.DELETION_VECTORS.specName(),
                    TableFeature.V2_CHECKPOINT.specName(),
                    TableFeature.IN_COMMIT_TIMESTAMP.specName()));
    Map<String, String> props = new HashMap<>();
    DeltaPropertyMapper.deriveFromProtocol(props, protocol);
    assertThat(props)
        .containsEntry(TableProperties.MIN_READER_VERSION, "3")
        .containsEntry(TableProperties.MIN_WRITER_VERSION, "7")
        .containsEntry(featureKey(TableFeature.CATALOG_MANAGED.specName()), "supported")
        .containsEntry(featureKey(TableFeature.DELETION_VECTORS.specName()), "supported")
        .containsEntry(featureKey(TableFeature.V2_CHECKPOINT.specName()), "supported")
        .containsEntry(featureKey(TableFeature.IN_COMMIT_TIMESTAMP.specName()), "supported");
  }

  @Test
  public void deriveFromProtocolNullIsNoop() {
    Map<String, String> props = new HashMap<>();
    DeltaPropertyMapper.deriveFromProtocol(props, null);
    assertThat(props).isEmpty();
  }

  @Test
  public void deriveFromProtocolEmptyFeatureListsStillEmitsVersionProps() {
    DeltaProtocol protocol = new DeltaProtocol().minReaderVersion(3).minWriterVersion(7);
    Map<String, String> props = new HashMap<>();
    DeltaPropertyMapper.deriveFromProtocol(props, protocol);
    assertThat(props)
        .containsOnly(
            entry(TableProperties.MIN_READER_VERSION, "3"),
            entry(TableProperties.MIN_WRITER_VERSION, "7"));
  }

  // ---------- deriveFromDomainMetadata ----------

  @Test
  public void deriveFromDomainMetadataClusteringEncodesAsJsonArrayOfPaths() throws Exception {
    DeltaDomainMetadataUpdates dm =
        new DeltaDomainMetadataUpdates()
            .deltaClustering(
                new DeltaClusteringDomainMetadata()
                    .clusteringColumns(List.of(List.of("id"), List.of("address", "city"))));
    Map<String, String> props = new HashMap<>();
    DeltaPropertyMapper.deriveFromDomainMetadata(props, dm);
    String json = props.get(TableProperties.CLUSTERING_COLUMNS);
    JsonNode parsed = JSON.readTree(json);
    // Nested paths stay as arrays -- don't collapse into dotted strings.
    assertThat(parsed.isArray()).isTrue();
    assertThat(parsed.size()).isEqualTo(2);
    assertThat(parsed.get(0).get(0).asText()).isEqualTo("id");
    assertThat(parsed.get(1).get(0).asText()).isEqualTo("address");
    assertThat(parsed.get(1).get(1).asText()).isEqualTo("city");
  }

  @Test
  public void deriveFromDomainMetadataRowTrackingEncodesHighWaterMark() {
    DeltaDomainMetadataUpdates dm =
        new DeltaDomainMetadataUpdates()
            .deltaRowTracking(new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(42L));
    Map<String, String> props = new HashMap<>();
    DeltaPropertyMapper.deriveFromDomainMetadata(props, dm);
    assertThat(props).containsEntry(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK, "42");
  }

  @Test
  public void deriveFromDomainMetadataNullIsNoop() {
    Map<String, String> props = new HashMap<>();
    DeltaPropertyMapper.deriveFromDomainMetadata(props, null);
    assertThat(props).isEmpty();
  }

  @Test
  public void deriveFromDomainMetadataOmitsUnsetEntries() {
    // A DeltaDomainMetadataUpdates with neither clustering nor row-tracking fields set yields no
    // properties. Pins the "only write properties for entries the client actually provided"
    // contract for the shared derive API.
    Map<String, String> props = new HashMap<>();
    DeltaPropertyMapper.deriveFromDomainMetadata(props, new DeltaDomainMetadataUpdates());
    assertThat(props).isEmpty();
  }

  // ---------- buildStoredProperties ----------

  @Test
  public void mergeDerivedPropertiesWinOnConflict() {
    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(3)
            .minWriterVersion(7)
            .writerFeatures(List.of(TableFeature.CATALOG_MANAGED.specName()));
    DeltaCreateTableRequest req =
        new DeltaCreateTableRequest()
            .protocol(protocol)
            .properties(
                Map.of(featureKey(TableFeature.CATALOG_MANAGED.specName()), "client-override"));
    Map<String, String> merged = DeltaPropertyMapper.buildStoredProperties(req);
    // Server-derived projection of the structured protocol block wins over a stray client entry
    // under the same key, so the structured block remains the single source of truth.
    assertThat(merged)
        .containsEntry(featureKey(TableFeature.CATALOG_MANAGED.specName()), "supported");
  }

  @Test
  public void mergeTolerantOfAllNulls() {
    // Empty request: no protocol, no domain-metadata, no properties, no timestamp. The only
    // entry produced is the unconditional delta.lastUpdateVersion=0 from buildStoredProperties.
    assertThat(DeltaPropertyMapper.buildStoredProperties(new DeltaCreateTableRequest()))
        .containsOnly(entry(TableProperties.LAST_UPDATE_VERSION, "0"));
  }

  @Test
  public void mergeCombinesProtocolDomainMetadataAndClient() {
    DeltaCreateTableRequest req =
        new DeltaCreateTableRequest()
            .protocol(
                new DeltaProtocol()
                    .minReaderVersion(3)
                    .minWriterVersion(7)
                    .writerFeatures(List.of(TableFeature.ROW_TRACKING.specName())))
            .domainMetadata(
                new DeltaDomainMetadataUpdates()
                    .deltaRowTracking(
                        new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(100L)))
            .properties(Map.of("custom.key", "custom.value"))
            .lastCommitTimestampMs(1700000000000L);
    Map<String, String> merged = DeltaPropertyMapper.buildStoredProperties(req);
    assertThat(merged)
        .containsEntry(featureKey(TableFeature.ROW_TRACKING.specName()), "supported")
        .containsEntry(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK, "100")
        .containsEntry("custom.key", "custom.value")
        .containsEntry(TableProperties.LAST_COMMIT_TIMESTAMP, "1700000000000")
        .containsEntry(TableProperties.LAST_UPDATE_VERSION, "0");
  }

  @Test
  public void mergeServerDerivedLastCommitTimestamp() {
    // The top-level last-commit-timestamp-ms field is the source of truth for the metastore-state
    // delta.lastCommitTimestamp property. A client-supplied delta.lastCommitTimestamp entry in
    // `properties` (today's OSS Delta + Kernel-UC pattern) gets overwritten by the server-derived
    // value so engines can't desynchronize the property bag from the catalog's view of v0.
    DeltaCreateTableRequest req =
        new DeltaCreateTableRequest()
            .properties(Map.of(TableProperties.LAST_COMMIT_TIMESTAMP, "999"))
            .lastCommitTimestampMs(1700000000000L);
    assertThat(DeltaPropertyMapper.buildStoredProperties(req))
        .containsEntry(TableProperties.LAST_COMMIT_TIMESTAMP, "1700000000000");
  }

  private static String featureKey(String feature) {
    return TableProperties.FEATURE_PREFIX + feature;
  }
}
