package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.delta.model.DeltaClusteringDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DeltaRowTrackingDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.ServerProperties;
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
    DeltaStagingTableResponse staging =
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
    assertThatCode(() -> validate(protocol, null, properties)).doesNotThrowAnyException();
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
    assertThatThrownBy(() -> validate(protocol, null, fullProperties()))
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
    assertThatThrownBy(() -> validate(protocol, null, fullProperties()))
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
    assertThatThrownBy(() -> validate(protocol, null, fullProperties()))
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
    assertThatThrownBy(() -> validate(protocol, null, fullProperties()))
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
    assertThatThrownBy(() -> validate(protocol, null, fullProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(
            "missing required reader-feature '" + TableFeature.CATALOG_MANAGED.specName() + "'");
  }

  // ---------- domain-metadata vs feature consistency ----------

  @Test
  public void validateRejectsClusteringDomainMetadataWithoutClusteringFeature() {
    DeltaDomainMetadataUpdates dm =
        new DeltaDomainMetadataUpdates()
            .deltaClustering(
                new DeltaClusteringDomainMetadata().clusteringColumns(List.of(List.of("id"))));
    assertThatThrownBy(() -> validate(fullProtocol(), dm, fullProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("'" + TableFeature.CLUSTERING.specName() + "' writer feature");
  }

  @Test
  public void validateRejectsRowTrackingDomainMetadataWithoutRowTrackingFeature() {
    DeltaDomainMetadataUpdates dm =
        new DeltaDomainMetadataUpdates()
            .deltaRowTracking(new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(0L));
    assertThatThrownBy(() -> validate(fullProtocol(), dm, fullProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("'" + TableFeature.ROW_TRACKING.specName() + "' writer feature");
  }

  @Test
  public void validateAcceptsDomainMetadataWithMatchingFeatures() {
    DeltaProtocol protocol =
        fullProtocolWithExtraWriterFeatures(
            TableFeature.CLUSTERING.specName(), TableFeature.ROW_TRACKING.specName());
    DeltaDomainMetadataUpdates dm =
        new DeltaDomainMetadataUpdates()
            .deltaClustering(
                new DeltaClusteringDomainMetadata().clusteringColumns(List.of(List.of("id"))))
            .deltaRowTracking(new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(7L));
    assertThatCode(() -> validate(protocol, dm, fullProperties())).doesNotThrowAnyException();
  }

  @Test
  public void validateAcceptsNullDomainMetadata() {
    assertThatCode(() -> validate(fullProtocol(), null, fullProperties()))
        .doesNotThrowAnyException();
  }

  // ---------- properties ----------

  @Test
  public void validateRejectsRequiredPropertyWithWrongValue() {
    Map<String, String> props = fullProperties();
    props.put(TableProperties.CHECKPOINT_POLICY, "v1"); // contract says v2
    assertThatThrownBy(() -> validate(fullProtocol(), null, props))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableProperties.CHECKPOINT_POLICY)
        .hasMessageContaining("'v2'")
        .hasMessageContaining("'v1'");
  }

  @Test
  public void validateRejectsMissingUcTableId() {
    Map<String, String> props = fullProperties();
    props.remove(TableProperties.UC_TABLE_ID);
    assertThatThrownBy(() -> validate(fullProtocol(), null, props))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableProperties.UC_TABLE_ID);
  }

  @Test
  public void validateRejectsBlankUcTableId() {
    Map<String, String> props = fullProperties();
    props.put(TableProperties.UC_TABLE_ID, "   ");
    assertThatThrownBy(() -> validate(fullProtocol(), null, props))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableProperties.UC_TABLE_ID);
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

  // ---------- skipDeletionVectorRequirement=true ----------

  @Test
  public void validateWithDvNotRequiredAcceptsProtocolAndPropertiesWithoutDv() {
    assertThatCode(
            () ->
                UcManagedDeltaContract.validate(
                    TestUtils.protocolWithoutDv(),
                    null,
                    TestUtils.propertiesWithoutDvAndWithIcebergCompatV2(),
                    TestUtils.serverPropertiesWithAllowMissingDv()))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateWithDvRequiredStillRejectsMissingDvFeature() {
    assertThatThrownBy(
            () ->
                UcManagedDeltaContract.validate(
                    TestUtils.protocolWithoutDv(),
                    null,
                    TestUtils.propertiesWithoutDv(),
                    new ServerProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableFeature.DELETION_VECTORS.specName());
  }

  @Test
  public void validateWithDvNotRequiredStillEnforcesOtherRequiredFeatures() {
    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(3)
            .minWriterVersion(7)
            // catalogManaged intentionally removed
            .readerFeatures(
                List.of(
                    TableFeature.V2_CHECKPOINT.specName(),
                    TableFeature.VACUUM_PROTOCOL_CHECK.specName()))
            .writerFeatures(
                List.of(
                    TableFeature.V2_CHECKPOINT.specName(),
                    TableFeature.VACUUM_PROTOCOL_CHECK.specName(),
                    TableFeature.IN_COMMIT_TIMESTAMP.specName()));
    assertThatThrownBy(
            () ->
                UcManagedDeltaContract.validate(
                    protocol,
                    null,
                    TestUtils.propertiesWithoutDvAndWithIcebergCompatV2(),
                    TestUtils.serverPropertiesWithAllowMissingDv()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableFeature.CATALOG_MANAGED.specName());
  }

  @Test
  public void validateWithDvNotRequiredStillEnforcesOtherRequiredProperties() {
    Map<String, String> props = TestUtils.propertiesWithoutDvAndWithIcebergCompatV2();
    props.put(TableProperties.CHECKPOINT_POLICY, "v1"); // contract says v2
    assertThatThrownBy(
            () ->
                UcManagedDeltaContract.validate(
                    TestUtils.protocolWithoutDv(),
                    null,
                    props,
                    TestUtils.serverPropertiesWithAllowMissingDv()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableProperties.CHECKPOINT_POLICY);
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
    Map<String, String> props = new HashMap<>(UcManagedDeltaContract.REQUIRED_FIXED_PROPERTIES);
    props.put(TableProperties.UC_TABLE_ID, UUID.randomUUID().toString());
    return props;
  }

  /** Delegates to the 4-param overload with default (flag-off) ServerProperties. */
  private static void validate(
      DeltaProtocol protocol,
      DeltaDomainMetadataUpdates domainMetadata,
      Map<String, String> properties) {
    UcManagedDeltaContract.validate(protocol, domainMetadata, properties, new ServerProperties());
  }
}
