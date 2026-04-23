package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.delta.model.ClusteringDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DomainMetadataUpdates;
import io.unitycatalog.server.delta.model.RowTrackingDomainMetadata;
import io.unitycatalog.server.delta.model.StagingTableResponse;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link UcManagedDeltaContract#validate} -- the contract that {@link
 * DeltaStagingTableMapper} advertises and future create / update / commit endpoints check against.
 * Tests are organized as: closed-loop happy path, then per-arm negative cases.
 */
public class UcManagedDeltaContractTest {

  /**
   * Closed loop: take the contract advertised by {@link DeltaStagingTableMapper} verbatim (echoed
   * back as the createTable request would carry it), fill in non-null values for the
   * engine-generated entries, and assert {@link UcManagedDeltaContract#validate} accepts. Pins that
   * the producer (staging mapper) and consumer (contract validator) agree on what "satisfies the
   * contract" means.
   */
  @Test
  public void validateAcceptsTheAdvertisedContractRoundTrip() {
    StagingTableResponse staging =
        DeltaStagingTableMapper.toStagingTableResponse(sampleStagingInfo(), emptyCredentials());
    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(staging.getRequiredProtocol().getMinReaderVersion())
            .minWriterVersion(staging.getRequiredProtocol().getMinWriterVersion())
            .readerFeatures(staging.getRequiredProtocol().getReaderFeatures())
            .writerFeatures(staging.getRequiredProtocol().getWriterFeatures());
    Map<String, String> properties = new HashMap<>();
    staging
        .getRequiredProperties()
        .forEach((k, v) -> properties.put(k, v != null ? v : "engine-supplied"));
    assertThatCode(() -> UcManagedDeltaContract.validate(protocol, null, properties))
        .doesNotThrowAnyException();
  }

  // ---------- reader-features subset of writer-features ----------

  @Test
  public void validateRejectsReaderFeatureNotInWriterFeatures() {
    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(3)
            .minWriterVersion(7)
            .readerFeatures(
                List.of(
                    TableFeature.CATALOG_MANAGED.specName(),
                    TableFeature.DELETION_VECTORS.specName(),
                    TableFeature.V2_CHECKPOINT.specName(),
                    TableFeature.VACUUM_PROTOCOL_CHECK.specName()))
            // V2_CHECKPOINT is in reader-features but missing from writer-features.
            .writerFeatures(
                List.of(
                    TableFeature.CATALOG_MANAGED.specName(),
                    TableFeature.DELETION_VECTORS.specName(),
                    TableFeature.IN_COMMIT_TIMESTAMP.specName(),
                    TableFeature.VACUUM_PROTOCOL_CHECK.specName()));
    assertThatThrownBy(() -> UcManagedDeltaContract.validate(protocol, null, fullProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableFeature.V2_CHECKPOINT.specName())
        .hasMessageContaining("reader-features but not writer-features");
  }

  // ---------- versions ----------

  @Test
  public void validateRejectsBelowMinReaderVersion() {
    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(2)
            .minWriterVersion(7)
            .readerFeatures(fullProtocol().getReaderFeatures())
            .writerFeatures(fullProtocol().getWriterFeatures());
    assertThatThrownBy(() -> UcManagedDeltaContract.validate(protocol, null, fullProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("minReaderVersion must be at least 3");
  }

  @Test
  public void validateRejectsBelowMinWriterVersion() {
    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(3)
            .minWriterVersion(6)
            .readerFeatures(fullProtocol().getReaderFeatures())
            .writerFeatures(fullProtocol().getWriterFeatures());
    assertThatThrownBy(() -> UcManagedDeltaContract.validate(protocol, null, fullProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("minWriterVersion must be at least 7");
  }

  // ---------- features ----------

  @Test
  public void validateRejectsMissingRequiredWriterFeature() {
    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(3)
            .minWriterVersion(7)
            .readerFeatures(
                List.of(
                    TableFeature.CATALOG_MANAGED.specName(),
                    TableFeature.DELETION_VECTORS.specName(),
                    TableFeature.V2_CHECKPOINT.specName(),
                    TableFeature.VACUUM_PROTOCOL_CHECK.specName()))
            // IN_COMMIT_TIMESTAMP intentionally missing.
            .writerFeatures(
                List.of(
                    TableFeature.CATALOG_MANAGED.specName(),
                    TableFeature.DELETION_VECTORS.specName(),
                    TableFeature.V2_CHECKPOINT.specName(),
                    TableFeature.VACUUM_PROTOCOL_CHECK.specName()));
    assertThatThrownBy(() -> UcManagedDeltaContract.validate(protocol, null, fullProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(
            "missing required writer-feature '"
                + TableFeature.IN_COMMIT_TIMESTAMP.specName()
                + "'");
  }

  @Test
  public void validateRejectsMissingRequiredReaderFeature() {
    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(3)
            .minWriterVersion(7)
            // CATALOG_MANAGED intentionally missing from reader-features (it's a reader-writer
            // feature, must appear in BOTH lists).
            .readerFeatures(
                List.of(
                    TableFeature.DELETION_VECTORS.specName(),
                    TableFeature.V2_CHECKPOINT.specName(),
                    TableFeature.VACUUM_PROTOCOL_CHECK.specName()))
            .writerFeatures(
                List.of(
                    TableFeature.CATALOG_MANAGED.specName(),
                    TableFeature.DELETION_VECTORS.specName(),
                    TableFeature.IN_COMMIT_TIMESTAMP.specName(),
                    TableFeature.V2_CHECKPOINT.specName(),
                    TableFeature.VACUUM_PROTOCOL_CHECK.specName()));
    assertThatThrownBy(() -> UcManagedDeltaContract.validate(protocol, null, fullProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(
            "missing required reader-feature '" + TableFeature.CATALOG_MANAGED.specName() + "'");
  }

  // ---------- domain-metadata vs feature consistency ----------

  @Test
  public void validateRejectsClusteringDomainMetadataWithoutClusteringFeature() {
    DomainMetadataUpdates dm =
        new DomainMetadataUpdates()
            .deltaClustering(
                new ClusteringDomainMetadata().clusteringColumns(List.of(List.of("id"))));
    assertThatThrownBy(() -> UcManagedDeltaContract.validate(fullProtocol(), dm, fullProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("'" + TableFeature.CLUSTERING.specName() + "' writer feature");
  }

  @Test
  public void validateRejectsRowTrackingDomainMetadataWithoutRowTrackingFeature() {
    DomainMetadataUpdates dm =
        new DomainMetadataUpdates()
            .deltaRowTracking(new RowTrackingDomainMetadata().rowIdHighWaterMark(0L));
    assertThatThrownBy(() -> UcManagedDeltaContract.validate(fullProtocol(), dm, fullProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("'" + TableFeature.ROW_TRACKING.specName() + "' writer feature");
  }

  @Test
  public void validateAcceptsDomainMetadataWithMatchingFeatures() {
    DeltaProtocol protocol =
        fullProtocolWithExtraWriterFeatures(
            TableFeature.CLUSTERING.specName(), TableFeature.ROW_TRACKING.specName());
    DomainMetadataUpdates dm =
        new DomainMetadataUpdates()
            .deltaClustering(
                new ClusteringDomainMetadata().clusteringColumns(List.of(List.of("id"))))
            .deltaRowTracking(new RowTrackingDomainMetadata().rowIdHighWaterMark(7L));
    assertThatCode(() -> UcManagedDeltaContract.validate(protocol, dm, fullProperties()))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateAcceptsNullDomainMetadata() {
    assertThatCode(() -> UcManagedDeltaContract.validate(fullProtocol(), null, fullProperties()))
        .doesNotThrowAnyException();
  }

  // ---------- properties ----------

  @Test
  public void validateRejectsRequiredPropertyWithWrongValue() {
    Map<String, String> props = fullProperties();
    props.put(TableProperties.CHECKPOINT_POLICY, "v1"); // contract says v2
    assertThatThrownBy(() -> UcManagedDeltaContract.validate(fullProtocol(), null, props))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableProperties.CHECKPOINT_POLICY)
        .hasMessageContaining("'v2'")
        .hasMessageContaining("'v1'");
  }

  @Test
  public void validateRejectsMissingUcTableId() {
    Map<String, String> props = fullProperties();
    props.remove(TableProperties.UC_TABLE_ID);
    assertThatThrownBy(() -> UcManagedDeltaContract.validate(fullProtocol(), null, props))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableProperties.UC_TABLE_ID);
  }

  @Test
  public void validateRejectsBlankUcTableId() {
    Map<String, String> props = fullProperties();
    props.put(TableProperties.UC_TABLE_ID, "   ");
    assertThatThrownBy(() -> UcManagedDeltaContract.validate(fullProtocol(), null, props))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableProperties.UC_TABLE_ID);
  }

  @Test
  public void validateRejectsNullEngineGeneratedProperty() {
    // The staging response delivers IN_COMMIT_TIMESTAMP_ENABLEMENT_* with null values; the client
    // must compute and substitute. Echoing the null back is a contract violation.
    Map<String, String> props = fullProperties();
    props.put(TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP, null);
    assertThatThrownBy(() -> UcManagedDeltaContract.validate(fullProtocol(), null, props))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP);
  }

  // ---------- validateTableIdProperty ----------

  @Test
  public void validateTableIdAcceptsMatching() {
    String id = UUID.randomUUID().toString();
    Map<String, String> props = Map.of(TableProperties.UC_TABLE_ID, id);
    assertThatCode(() -> UcManagedDeltaContract.validateTableIdProperty(props, id))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateTableIdRejectsMismatch() {
    String expected = UUID.randomUUID().toString();
    String wrong = UUID.randomUUID().toString();
    Map<String, String> props = Map.of(TableProperties.UC_TABLE_ID, wrong);
    assertThatThrownBy(() -> UcManagedDeltaContract.validateTableIdProperty(props, expected))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableProperties.UC_TABLE_ID)
        .hasMessageContaining(wrong)
        .hasMessageContaining(expected);
  }

  @Test
  public void validateTableIdRejectsMissingProperty() {
    String expected = UUID.randomUUID().toString();
    assertThatThrownBy(() -> UcManagedDeltaContract.validateTableIdProperty(Map.of(), expected))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableProperties.UC_TABLE_ID);
  }

  @Test
  public void validateTableIdRejectsNullPropertiesMap() {
    String expected = UUID.randomUUID().toString();
    assertThatThrownBy(() -> UcManagedDeltaContract.validateTableIdProperty(null, expected))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableProperties.UC_TABLE_ID);
  }

  // --- fixtures ---

  private static StagingTableInfo sampleStagingInfo() {
    return new StagingTableInfo()
        .id(UUID.randomUUID().toString())
        .stagingLocation("s3://bucket/staging/" + UUID.randomUUID())
        .name("tbl")
        .catalogName("cat")
        .schemaName("sch");
  }

  private static TemporaryCredentials emptyCredentials() {
    return new TemporaryCredentials();
  }

  private static DeltaProtocol fullProtocol() {
    return new DeltaProtocol()
        .minReaderVersion(3)
        .minWriterVersion(7)
        .readerFeatures(
            List.of(
                TableFeature.CATALOG_MANAGED.specName(),
                TableFeature.DELETION_VECTORS.specName(),
                TableFeature.V2_CHECKPOINT.specName(),
                TableFeature.VACUUM_PROTOCOL_CHECK.specName()))
        .writerFeatures(
            List.of(
                TableFeature.CATALOG_MANAGED.specName(),
                TableFeature.DELETION_VECTORS.specName(),
                TableFeature.IN_COMMIT_TIMESTAMP.specName(),
                TableFeature.V2_CHECKPOINT.specName(),
                TableFeature.VACUUM_PROTOCOL_CHECK.specName()));
  }

  /** Full UC-managed protocol plus extra writer-only features (e.g. clustering, rowTracking). */
  private static DeltaProtocol fullProtocolWithExtraWriterFeatures(String... extraWriterFeatures) {
    DeltaProtocol p = fullProtocol();
    java.util.ArrayList<String> writers = new java.util.ArrayList<>(p.getWriterFeatures());
    for (String f : extraWriterFeatures) writers.add(f);
    return p.writerFeatures(writers);
  }

  private static Map<String, String> fullProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(TableProperties.CHECKPOINT_POLICY, "v2");
    props.put(TableProperties.CHECKPOINT_WRITE_STATS_AS_JSON, "false");
    props.put(TableProperties.CHECKPOINT_WRITE_STATS_AS_STRUCT, "true");
    props.put(TableProperties.ENABLE_DELETION_VECTORS, "true");
    props.put(TableProperties.ENABLE_IN_COMMIT_TIMESTAMPS, "true");
    props.put(TableProperties.UC_TABLE_ID, UUID.randomUUID().toString());
    props.put(TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, "0");
    props.put(TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP, "1700000000000");
    return props;
  }
}
