package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.delta.model.ClusteringDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DomainMetadataUpdates;
import io.unitycatalog.server.delta.model.RowTrackingDomainMetadata;
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
  public void deriveFromProtocolEmptyFeatureListsIsNoop() {
    DeltaProtocol protocol = new DeltaProtocol().minReaderVersion(3).minWriterVersion(7);
    Map<String, String> props = new HashMap<>();
    DeltaPropertyMapper.deriveFromProtocol(props, protocol);
    assertThat(props).isEmpty();
  }

  // ---------- deriveFromDomainMetadata ----------

  @Test
  public void deriveFromDomainMetadataClusteringEncodesAsJsonArrayOfPaths() throws Exception {
    DomainMetadataUpdates dm =
        new DomainMetadataUpdates()
            .deltaClustering(
                new ClusteringDomainMetadata()
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
    DomainMetadataUpdates dm =
        new DomainMetadataUpdates()
            .deltaRowTracking(new RowTrackingDomainMetadata().rowIdHighWaterMark(42L));
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
    // A DomainMetadataUpdates with neither clustering nor row-tracking fields set yields no
    // properties. Pins the "only write properties for entries the client actually provided"
    // contract for the shared derive API.
    Map<String, String> props = new HashMap<>();
    DeltaPropertyMapper.deriveFromDomainMetadata(props, new DomainMetadataUpdates());
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
    Map<String, String> client =
        Map.of(featureKey(TableFeature.CATALOG_MANAGED.specName()), "client-override");
    Map<String, String> merged = DeltaPropertyMapper.buildStoredProperties(protocol, null, client);
    // Server-derived projection of the structured protocol block wins over a stray client entry
    // under the same key, so the structured block remains the single source of truth.
    assertThat(merged)
        .containsEntry(featureKey(TableFeature.CATALOG_MANAGED.specName()), "supported");
  }

  @Test
  public void mergeTolerantOfAllNulls() {
    assertThat(DeltaPropertyMapper.buildStoredProperties(null, null, null)).isEmpty();
  }

  @Test
  public void mergeCombinesProtocolDomainMetadataAndClient() {
    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(3)
            .minWriterVersion(7)
            .writerFeatures(List.of(TableFeature.ROW_TRACKING.specName()));
    DomainMetadataUpdates dm =
        new DomainMetadataUpdates()
            .deltaRowTracking(new RowTrackingDomainMetadata().rowIdHighWaterMark(100L));
    Map<String, String> client = Map.of("custom.key", "custom.value");
    Map<String, String> merged = DeltaPropertyMapper.buildStoredProperties(protocol, dm, client);
    assertThat(merged)
        .containsEntry(featureKey(TableFeature.ROW_TRACKING.specName()), "supported")
        .containsEntry(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK, "100")
        .containsEntry("custom.key", "custom.value");
  }

  private static String featureKey(String feature) {
    return TableProperties.FEATURE_PREFIX + feature;
  }
}
