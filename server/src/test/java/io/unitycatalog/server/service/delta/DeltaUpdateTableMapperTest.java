package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.delta.model.DeltaAssertEtag;
import io.unitycatalog.server.delta.model.DeltaAssertTableUUID;
import io.unitycatalog.server.delta.model.DeltaClusteringDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DeltaRemoveDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.DeltaRemovePropertiesUpdate;
import io.unitycatalog.server.delta.model.DeltaRowTrackingDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaSetDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.DeltaSetPropertiesUpdate;
import io.unitycatalog.server.delta.model.DeltaSetProtocolUpdate;
import io.unitycatalog.server.delta.model.DeltaTableRequirement;
import io.unitycatalog.server.delta.model.DeltaTableUpdate;
import io.unitycatalog.server.delta.model.DeltaUpdateTableRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.MutablePropertyMap;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.service.delta.DeltaUpdateTableMapper.CollectedRequest;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DeltaUpdateTableMapper}'s session-independent paths -- request
 * classification ({@link DeltaUpdateTableMapper#collectRequest}) and {@code assert-*} requirement
 * checking against a DAO that lives only in memory.
 */
public class DeltaUpdateTableMapperTest {

  @Test
  public void collectRequestRejectsNullBody() {
    assertThatThrownBy(() -> DeltaUpdateTableMapper.collectRequest(null))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Request body is required");
  }

  @Test
  public void collectRequestRejectsEmptyUpdates() {
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.collectRequest(
                    new DeltaUpdateTableRequest()
                        .requirements(
                            List.of(
                                new DeltaAssertTableUUID()
                                    .uuid(UUID.randomUUID())
                                    .type("assert-table-uuid")))
                        .updates(List.of())))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("At least one update is required");
  }

  @Test
  public void collectRequestRejectsMissingAssertTableUuid() {
    // Null requirements list
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.collectRequest(
                    new DeltaUpdateTableRequest()
                        .updates(
                            List.of(
                                new DeltaSetPropertiesUpdate()
                                    .updates(Map.of("k", "v"))
                                    .action("set-properties")))))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("assert-table-uuid requirement is required");

    // Empty requirements list
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.collectRequest(
                    new DeltaUpdateTableRequest()
                        .requirements(List.of())
                        .updates(
                            List.of(
                                new DeltaSetPropertiesUpdate()
                                    .updates(Map.of("k", "v"))
                                    .action("set-properties")))))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("assert-table-uuid requirement is required");

    // Requirements list that has an etag but no UUID assertion
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.collectRequest(
                    new DeltaUpdateTableRequest()
                        .requirements(
                            List.of(new DeltaAssertEtag().etag("etag-x").type("assert-etag")))
                        .updates(
                            List.of(
                                new DeltaSetPropertiesUpdate()
                                    .updates(Map.of("k", "v"))
                                    .action("set-properties")))))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("assert-table-uuid requirement is required");
  }

  @Test
  public void collectRequestRejectsDuplicateAssertTableUuid() {
    UUID id = UUID.randomUUID();
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.collectRequest(
                    new DeltaUpdateTableRequest()
                        .requirements(
                            List.of(
                                new DeltaAssertTableUUID().uuid(id).type("assert-table-uuid"),
                                new DeltaAssertTableUUID().uuid(id).type("assert-table-uuid")))
                        .updates(
                            List.of(
                                new DeltaSetPropertiesUpdate()
                                    .updates(Map.of("k", "v"))
                                    .action("set-properties")))))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("At most one assert-table-uuid is allowed per request");
  }

  @Test
  public void collectRequestRejectsSetRemovePropertiesOverlap() {
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.collectRequest(
                    new DeltaUpdateTableRequest()
                        .requirements(
                            List.of(
                                new DeltaAssertTableUUID()
                                    .uuid(UUID.randomUUID())
                                    .type("assert-table-uuid")))
                        .updates(
                            List.of(
                                new DeltaSetPropertiesUpdate()
                                    .updates(Map.of("k", "v"))
                                    .action("set-properties"),
                                new DeltaRemovePropertiesUpdate()
                                    .removals(List.of("k"))
                                    .action("remove-properties")))))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("set-properties and remove-properties overlap");
  }

  @Test
  public void collectRequestRejectsSetRemoveDomainMetadataOverlap() {
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.collectRequest(
                    new DeltaUpdateTableRequest()
                        .requirements(
                            List.of(
                                new DeltaAssertTableUUID()
                                    .uuid(UUID.randomUUID())
                                    .type("assert-table-uuid")))
                        .updates(
                            List.of(
                                new DeltaSetDomainMetadataUpdate()
                                    .updates(
                                        new DeltaDomainMetadataUpdates()
                                            .deltaClustering(
                                                new DeltaClusteringDomainMetadata()
                                                    .clusteringColumns(List.of(List.of("c")))))
                                    .action("set-domain-metadata"),
                                new DeltaRemoveDomainMetadataUpdate()
                                    .domains(List.of("delta.clustering"))
                                    .action("remove-domain-metadata")))))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("set-domain-metadata and remove-domain-metadata overlap");
  }

  @Test
  public void collectRequestAcceptsNonEmptyUpdatesWithUuidRequirement() {
    DeltaUpdateTableRequest req =
        new DeltaUpdateTableRequest()
            .requirements(
                List.of(
                    new DeltaAssertTableUUID().uuid(UUID.randomUUID()).type("assert-table-uuid")))
            .updates(
                List.of(
                    new DeltaSetPropertiesUpdate()
                        .updates(Map.of("k", "v"))
                        .action("set-properties")));
    assertThatCode(() -> DeltaUpdateTableMapper.collectRequest(req)).doesNotThrowAnyException();
  }

  @Test
  public void assertTableUuidHappyPath() {
    UUID id = UUID.randomUUID();
    TableInfoDAO dao = new TableInfoDAO();
    dao.setId(id);
    dao.setUpdatedAt(new Date());

    assertThatCode(
            () ->
                DeltaUpdateTableMapper.checkTableUuidRequirement(
                    dao,
                    collectRequestFor(
                        new DeltaAssertTableUUID().uuid(id).type("assert-table-uuid"))))
        .doesNotThrowAnyException();
  }

  @Test
  public void assertTableUuidMismatchSurfacesUpdateRequirementConflict() {
    TableInfoDAO dao = new TableInfoDAO();
    dao.setId(UUID.randomUUID());
    dao.setUpdatedAt(new Date());

    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.checkTableUuidRequirement(
                    dao,
                    collectRequestFor(
                        new DeltaAssertTableUUID()
                            .uuid(UUID.randomUUID())
                            .type("assert-table-uuid"))))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("assert-table-uuid failed");
  }

  @Test
  public void assertEtagHappyPath() {
    // The client's assert-etag matches the pre-apply etag, so the check passes.
    String preApplyEtag = "etag-1700000000000";
    assertThatCode(
            () ->
                DeltaUpdateTableMapper.checkEtagRequirement(
                    preApplyEtag,
                    collectRequestFor(
                        new DeltaAssertTableUUID()
                            .uuid(UUID.randomUUID())
                            .type("assert-table-uuid"),
                        new DeltaAssertEtag().etag(preApplyEtag).type("assert-etag"))))
        .doesNotThrowAnyException();
  }

  @Test
  public void assertEtagMismatchSurfacesUpdateRequirementConflict() {
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.checkEtagRequirement(
                    "etag-current",
                    collectRequestFor(
                        new DeltaAssertTableUUID()
                            .uuid(UUID.randomUUID())
                            .type("assert-table-uuid"),
                        new DeltaAssertEtag().etag("etag-stale").type("assert-etag"))))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("assert-etag failed");
  }

  // ---------- applyUpdates: skipDeletionVectorRequirement flag ----------

  @Test
  public void applyUpdatesSetProtocolRejectsMissingDvWhenFlagOff() {
    // Default ServerProperties has allow-missing-dv=false, so DV is required.
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.applyUpdates(
                    null,
                    managedDao(),
                    propsFrom(TestUtils.propertiesWithoutDv()),
                    collectSetProtocolRequest(TestUtils.protocolWithoutDv()),
                    new ServerProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableFeature.DELETION_VECTORS.specName());
  }

  @Test
  public void applyUpdatesSetProtocolAcceptsMissingDvWhenFlagOn() {
    // allow-missing-dv=true + IcebergCompatV2 property already in existing table properties →
    // property survives set-protocol (which only replaces delta.feature.* keys), DV check skipped.
    assertThatCode(
            () ->
                DeltaUpdateTableMapper.applyUpdates(
                    null,
                    managedDao(),
                    propsFrom(TestUtils.propertiesWithoutDvAndWithIcebergCompatV2()),
                    collectSetProtocolRequest(TestUtils.protocolWithoutDv()),
                    TestUtils.serverPropertiesWithAllowMissingDv()))
        .doesNotThrowAnyException();
  }

  @Test
  public void applyUpdatesSetProtocolAndPropertiesAcceptsMissingDvWhenPropertyAddedInSameCommit() {
    // allow-missing-dv=true + existing table has no IcebergCompatV2 property, but the same commit
    // adds it via set-properties → post-apply properties have it, DV check skipped.
    assertThatCode(
            () ->
                DeltaUpdateTableMapper.applyUpdates(
                    null,
                    managedDao(),
                    propsFrom(TestUtils.propertiesWithoutDv()),
                    collectSetProtocolAndSetPropertiesRequest(
                        TestUtils.protocolWithoutDv(),
                        Map.of(TableProperties.ENABLE_ICEBERG_COMPAT_V2, "true")),
                    TestUtils.serverPropertiesWithAllowMissingDv()))
        .doesNotThrowAnyException();
  }

  @Test
  public void applyUpdatesSetProtocolStillRequiresDvWhenIcebergCompatV2PropertyAbsent() {
    // allow-missing-dv=true but delta.enableIcebergCompatV2 not in map → DV still required.
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.applyUpdates(
                    null,
                    managedDao(),
                    propsFrom(TestUtils.propertiesWithoutDv()),
                    collectSetProtocolRequest(TestUtils.protocolWithoutDv()),
                    TestUtils.serverPropertiesWithAllowMissingDv()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableFeature.DELETION_VECTORS.specName());
  }

  @Test
  public void applyUpdatesSetProtocolRejectsMissingDvWhenFlagOffEvenWithIcebergCompatV2Property() {
    // flag=false, delta.enableIcebergCompatV2 present → DV still required (flag gates the skip).
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.applyUpdates(
                    null,
                    managedDao(),
                    propsFrom(TestUtils.propertiesWithoutDvAndWithIcebergCompatV2()),
                    collectSetProtocolRequest(TestUtils.protocolWithoutDv()),
                    new ServerProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableFeature.DELETION_VECTORS.specName());
  }

  @Test
  public void changesTableMetadataExemptsRowTrackingOnlyDomainMetadata() {
    // The Delta protocol requires writers to advance the rowTracking high-water mark on every
    // fresh-row commit, so an HWM-only set-domain-metadata rides along with data commits and
    // must not classify the request as a metadata change.
    DeltaSetDomainMetadataUpdate hwmOnly =
        new DeltaSetDomainMetadataUpdate()
            .updates(
                new DeltaDomainMetadataUpdates()
                    .deltaRowTracking(new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(7L)))
            .action("set-domain-metadata");
    assertThat(collectUpdatesRequest(hwmOnly).updates().changesTableMetadata()).isFalse();

    // A set-domain-metadata with no updates body sets nothing and is likewise not a change.
    assertThat(
            collectUpdatesRequest(new DeltaSetDomainMetadataUpdate().action("set-domain-metadata"))
                .updates()
                .changesTableMetadata())
        .isFalse();

    // Touching any other domain in the same action still counts...
    assertThat(
            collectUpdatesRequest(
                    new DeltaSetDomainMetadataUpdate()
                        .updates(
                            new DeltaDomainMetadataUpdates()
                                .deltaRowTracking(
                                    new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(7L))
                                .deltaClustering(
                                    new DeltaClusteringDomainMetadata()
                                        .clusteringColumns(List.of(List.of("id")))))
                        .action("set-domain-metadata"))
                .updates()
                .changesTableMetadata())
        .isTrue();

    // ... as does any sibling metadata action alongside the HWM.
    assertThat(
            collectUpdatesRequest(
                    hwmOnly,
                    new DeltaSetPropertiesUpdate()
                        .updates(Map.of("k", "v"))
                        .action("set-properties"))
                .updates()
                .changesTableMetadata())
        .isTrue();
  }

  // --- fixtures ---

  /** Build a {@link CollectedRequest} with the canonical requirement and the given updates. */
  private static CollectedRequest collectUpdatesRequest(DeltaTableUpdate... updates) {
    return DeltaUpdateTableMapper.collectRequest(
        new DeltaUpdateTableRequest()
            .requirements(
                List.of(
                    new DeltaAssertTableUUID().uuid(UUID.randomUUID()).type("assert-table-uuid")))
            .updates(List.of(updates)));
  }

  /** Build a {@link CollectedRequest} with the supplied requirements and a no-op update. */
  private static CollectedRequest collectRequestFor(DeltaTableRequirement... requirements) {
    return DeltaUpdateTableMapper.collectRequest(
        new DeltaUpdateTableRequest()
            .requirements(List.of(requirements))
            .updates(
                List.of(
                    new DeltaSetPropertiesUpdate()
                        .updates(Map.of("k", "v"))
                        .action("set-properties"))));
  }

  private static CollectedRequest collectSetProtocolRequest(DeltaProtocol protocol) {
    return collectUpdatesRequest(
        new DeltaSetProtocolUpdate().protocol(protocol).action("set-protocol"));
  }

  private static CollectedRequest collectSetProtocolAndSetPropertiesRequest(
      DeltaProtocol protocol, Map<String, String> extraProperties) {
    return collectUpdatesRequest(
        new DeltaSetProtocolUpdate().protocol(protocol).action("set-protocol"),
        new DeltaSetPropertiesUpdate().updates(extraProperties).action("set-properties"));
  }

  private static TableInfoDAO managedDao() {
    TableInfoDAO dao = new TableInfoDAO();
    dao.setId(UUID.randomUUID());
    dao.setType("MANAGED");
    dao.setUpdatedAt(new Date());
    return dao;
  }

  private static MutablePropertyMap propsFrom(Map<String, String> map) {
    UUID id = UUID.randomUUID();
    List<PropertyDAO> daos =
        map.entrySet().stream()
            .map(
                e ->
                    PropertyDAO.builder()
                        .entityId(id)
                        .entityType("table")
                        .key(e.getKey())
                        .value(e.getValue())
                        .build())
            .toList();
    return MutablePropertyMap.wrap(daos);
  }
}
