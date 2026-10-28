package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.delta.model.DeltaArrayType;
import io.unitycatalog.server.delta.model.DeltaAssertEtag;
import io.unitycatalog.server.delta.model.DeltaAssertTableUUID;
import io.unitycatalog.server.delta.model.DeltaClusteringDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaDataType;
import io.unitycatalog.server.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.server.delta.model.DeltaMapType;
import io.unitycatalog.server.delta.model.DeltaPrimitiveType;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DeltaRemoveDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.DeltaRemovePropertiesUpdate;
import io.unitycatalog.server.delta.model.DeltaRowTrackingDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaSetDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.DeltaSetPropertiesUpdate;
import io.unitycatalog.server.delta.model.DeltaSetProtocolUpdate;
import io.unitycatalog.server.delta.model.DeltaSetSchemaUpdate;
import io.unitycatalog.server.delta.model.DeltaSetTableCommentUpdate;
import io.unitycatalog.server.delta.model.DeltaStructField;
import io.unitycatalog.server.delta.model.DeltaStructFieldMetadata;
import io.unitycatalog.server.delta.model.DeltaStructType;
import io.unitycatalog.server.delta.model.DeltaTableRequirement;
import io.unitycatalog.server.delta.model.DeltaTableUpdate;
import io.unitycatalog.server.delta.model.DeltaUpdateTableRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.MutablePropertyMap;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.service.delta.DeltaUpdateTableMapper.CollectedRequest;
import io.unitycatalog.server.utils.ColumnUtils;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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

  // ---------- stored column-mapping mode ----------

  @Test
  public void invalidStoredColumnMappingModeIsIgnoredWithoutSetColumns() {
    // The mode is only read for set-columns, so a comment update on such a table still applies.
    assertThatCode(
            () ->
                DeltaUpdateTableMapper.applyUpdates(
                    null,
                    managedDao(),
                    propsFrom(Map.of(TableProperties.COLUMN_MAPPING_MODE, "foo")),
                    collectUpdatesRequest(
                        new DeltaSetTableCommentUpdate().comment("c").action("set-table-comment")),
                    new ServerProperties()))
        .doesNotThrowAnyException();
  }

  // --- set-columns column-mapping guards ---

  /**
   * Rejections from the set-columns column-mapping guards. Each one is raised in the mapper before
   * anything is stored, so the cases run against an in-memory table. {@code SdkUpdateTableTest}
   * keeps one rejection that goes through the server, and the accepted cases.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("guardRejections")
  public void testColumnMappingGuardRejections(GuardCase c) {
    assertThatThrownBy(
            () ->
                DeltaUpdateTableMapper.applyUpdates(
                    null,
                    c.table().dao(),
                    propsFrom(c.table().properties()),
                    collectUpdatesRequest(c.update()),
                    new ServerProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(c.rejection())
        .extracting("errorCode")
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);
  }

  static Stream<GuardCase> guardRejections() {
    return Stream.of(
        // Enabling needs a physical name on every column and nested field.
        rejected(
            "enable without a physical name on one column",
            "requires a physical column name",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "id"), fld("amount", prim("double"), 2, null))),
        rejected(
            "enable with a null physical name",
            "requires a physical column name",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "id"),
                fld("amount", prim("double"), 2, JSON_NULL))),
        rejected(
            "enable with a numeric physical name",
            "requires a physical column name",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "id"), fld("amount", prim("double"), 2, 5))),
        rejected(
            "enable without a nested physical name",
            "requires a physical column name",
            EXTERNAL_STRUCT,
            cmMode("name"),
            setColumns(fld("s", struct(fld("x", prim("long"), 2, null)), false, 1, "s"))),

        // A key kept only below the top level is not "column mapping removed"; the per-column
        // check reports the column that lost it.
        rejected(
            "key kept only in a struct field",
            "dropped column mapping metadata for column(s)",
            NAME_S,
            setColumns(fld("s", struct(fld("x", prim("long"), 2, "col-x")), null, null))),
        rejected(
            "key kept in one of two struct fields",
            "dropped column mapping metadata for column(s)",
            NAME_S2,
            setColumns(
                fld(
                    "s",
                    struct(fld("x", prim("long"), 2, "col-x"), fld("z", prim("long"), null, null)),
                    null,
                    null))),
        rejected(
            "key kept only in an array element field",
            "dropped column mapping metadata for column(s)",
            managed(
                "name", 2, fld("e", array(struct(fld("x", prim("long"), 2, "col-x"))), 1, "col-e")),
            setColumns(fld("e", array(struct(fld("x", prim("long"), 2, "col-x"))), null, null))),
        rejected(
            "key kept only in a map value field",
            "dropped column mapping metadata for column(s)",
            managed(
                "name",
                2,
                fld(
                    "m",
                    map(prim("string"), struct(fld("x", prim("long"), 2, "col-x"))),
                    1,
                    "col-m")),
            setColumns(
                fld(
                    "m",
                    map(prim("string"), struct(fld("x", prim("long"), 2, "col-x"))),
                    null,
                    null))),

        // Enabling with a shared id.
        rejected(
            "enable with a shared id",
            "Duplicate column mapping id",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "id"),
                fld("amount", prim("double"), 1, "amount"))),

        // Enabling with a malformed id.
        rejected(
            "enable with a boolean id",
            "malformed column mapping identity",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, true, "id"),
                fld("amount", prim("double"), 2, "amount"))),
        rejected(
            "enable with an object id",
            "malformed column mapping identity",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, Map.of(), "id"),
                fld("amount", prim("double"), 2, "amount"))),
        rejected(
            "enable with an empty-string id",
            "malformed column mapping identity",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, "", "id"),
                fld("amount", prim("double"), 2, "amount"))),
        rejected(
            "enable with a null id",
            "malformed column mapping identity",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, JSON_NULL, "id"),
                fld("amount", prim("double"), 2, "amount"))),
        rejected(
            "enable with a null nested id",
            "malformed column mapping identity",
            EXTERNAL_STRUCT,
            cmMode("name"),
            setColumns(fld("s", struct(fld("x", prim("long"), JSON_NULL, "x")), false, 1, "s"))),
        // A null id would be stored, and Kernel fails on it once a rename runs the check.
        rejected(
            "null id in name mode",
            "malformed column mapping identity",
            NAME_A,
            setColumns(fld("a", prim("long"), false, JSON_NULL, "col-1"))),
        rejected(
            "null nested id in name mode",
            "malformed column mapping identity",
            NAME_S,
            setColumns(fld("s", struct(fld("x", prim("long"), JSON_NULL, "col-x")), 1, "col-s"))),
        rejected(
            "null nested id with a rename in name mode",
            "malformed column mapping identity",
            NAME_S,
            setColumns(fld("t", struct(fld("x", prim("long"), JSON_NULL, "col-x")), 1, "col-s"))),

        // Enabling with a shared physical name.
        rejected(
            "enable with a shared physical name",
            "Duplicate column mapping physicalName",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "col-x"),
                fld("amount", prim("double"), 2, "col-x"))),

        // Duplicate physical names are compared without case.
        rejected(
            "physical names differing only by case",
            "Duplicate column mapping physicalName",
            NAME_AB,
            setColumns(
                fld("a", prim("long"), false, 1, "col-dup"), fld("b", prim("long"), 2, "COL-DUP"))),

        // A column-mapped table needs usable identity values.
        rejected(
            "empty physical name",
            "malformed column mapping identity",
            NAME_A,
            setColumns(fld("a", prim("long"), false, 1, ""))),
        rejected(
            "numeric physical name",
            "malformed column mapping identity",
            NAME_A,
            setColumns(fld("a", prim("long"), false, 1, 5))),
        rejected(
            "null physical name",
            "dropped column mapping metadata for column(s): a",
            NAME_A,
            setColumns(fld("a", prim("long"), false, 1, JSON_NULL))),
        rejected(
            "object physical name",
            "malformed column mapping identity",
            NAME_A,
            setColumns(fld("a", prim("long"), false, 1, Map.of()))),
        rejected(
            "empty-string id",
            "malformed column mapping identity",
            ID_A,
            setColumns(fld("a", prim("long"), false, "", "col-1"))),
        rejected(
            "object id",
            "malformed column mapping identity",
            ID_A,
            setColumns(fld("a", prim("long"), false, Map.of(), "col-1"))),
        rejected(
            "boolean id",
            "malformed column mapping identity",
            ID_A,
            setColumns(fld("a", prim("long"), false, true, "col-1"))),

        // Two empty physical names are malformed, not duplicates.
        rejected(
            "two empty physical names",
            "malformed column mapping identity",
            NAME_AB,
            setColumns(fld("a", prim("long"), false, 1, ""), fld("b", prim("long"), 2, ""))),

        // Ids are compared as text: 1 and "1" are one id, while "01" and "2147483648" are other
        // ids.
        rejected(
            "ids 1 and \"1\"",
            "Duplicate column mapping id '1'",
            ID_A,
            setColumns(
                fld("a", prim("long"), false, 1, "col-1"), fld("b", prim("long"), "1", "col-b"))),

        // A schema that has keys keeps them, even with no retained column.
        rejected(
            "drop every column and the keys",
            "set-columns dropped column mapping metadata; removing column mapping",
            NAME_A,
            setColumns(fld("z", prim("long"), false, null, null))),

        // In id mode renames are found by id.
        rejected(
            "two renames in id mode",
            "single top-level column rename",
            managed(
                "id",
                2,
                fld("a", prim("long"), false, 1, "col-1"),
                fld("b", prim("long"), 2, "col-2")),
            setColumns(
                fld("x", prim("long"), false, 1, "col-1"), fld("y", prim("long"), 2, "col-2"))),

        // Enabling and renaming in one request.
        rejected(
            "enable and rename a column",
            "separate statement",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "id"),
                fld("revenue", prim("double"), 2, "amount"))),
        rejected(
            "enable and rename a struct field",
            "separate statement",
            EXTERNAL_STRUCT,
            cmMode("name"),
            setColumns(fld("s", struct(fld("y", prim("long"), 2, "x")), false, 1, "s"))),
        // Enabling finds renames inside array elements and map values too.
        rejected(
            "enable and rename an array element field",
            "separate statement",
            external(
                fld("id", prim("long"), false, null, null),
                fld("amount", prim("double"), null, null),
                fld("e", array(struct(fld("x", prim("long"), null, null))), null, null)),
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "id"),
                fld("amount", prim("double"), 2, "amount"),
                fld("e", array(struct(fld("y", prim("long"), 4, "x"))), 3, "e"))),
        rejected(
            "enable and rename a map value field",
            "separate statement",
            external(
                fld("id", prim("long"), false, null, null),
                fld("amount", prim("double"), null, null),
                fld(
                    "m",
                    map(prim("string"), struct(fld("x", prim("long"), null, null))),
                    null,
                    null)),
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "id"),
                fld("amount", prim("double"), 2, "amount"),
                fld("m", map(prim("string"), struct(fld("y", prim("long"), 4, "x"))), 3, "m"))));
  }

  /** A column-mapping key sent with an explicit JSON null value (as opposed to left out). */
  private static final Object JSON_NULL = new Object();

  /** A stored table: its type, its properties and its columns. */
  private record StoredTable(
      String type, Map<String, String> properties, DeltaStructField... columns) {
    TableInfoDAO dao() {
      TableInfoDAO dao = managedDao();
      dao.setType(type);
      dao.setColumns(
          ColumnUtils.toColumnInfos(List.of(columns)).stream()
              .map(ColumnInfoDAO::from)
              .collect(Collectors.toList()));
      return dao;
    }
  }

  /** A set-columns request and the message its rejection contains. */
  private record GuardCase(
      String name, String rejection, StoredTable table, DeltaTableUpdate... update) {
    @Override
    public String toString() {
      return name;
    }
  }

  private static GuardCase rejected(
      String name, String rejection, StoredTable table, DeltaTableUpdate... update) {
    return new GuardCase(name, rejection, table, update);
  }

  /** A MANAGED table in the given column-mapping mode with the given columns. */
  private static StoredTable managed(String mode, int maxColumnId, DeltaStructField... fields) {
    return new StoredTable(
        "MANAGED",
        Map.of(
            TableProperties.COLUMN_MAPPING_MODE,
            mode,
            "delta.columnMapping.maxColumnId",
            Integer.toString(maxColumnId)),
        fields);
  }

  /** An EXTERNAL table without column mapping. */
  private static StoredTable external(DeltaStructField... fields) {
    return new StoredTable("EXTERNAL", Map.of(), fields);
  }

  private static final StoredTable EXTERNAL =
      external(
          fld("id", prim("long"), false, null, null), fld("amount", prim("double"), null, null));
  private static final StoredTable EXTERNAL_STRUCT =
      external(fld("s", struct(fld("x", prim("long"), null, null)), false, null, null));
  private static final StoredTable NAME_A =
      managed("name", 1, fld("a", prim("long"), false, 1, "col-1"));
  private static final StoredTable ID_A =
      managed("id", 1, fld("a", prim("long"), false, 1, "col-1"));
  private static final StoredTable NAME_AB =
      managed(
          "name", 2, fld("a", prim("long"), false, 1, "col-1"), fld("b", prim("long"), 2, "col-2"));
  private static final StoredTable NAME_S =
      managed("name", 2, fld("s", struct(fld("x", prim("long"), 2, "col-x")), 1, "col-s"));
  private static final StoredTable NAME_S2 =
      managed(
          "name",
          3,
          fld(
              "s",
              struct(fld("x", prim("long"), 2, "col-x"), fld("z", prim("long"), 3, "col-z")),
              1,
              "col-s"));

  private static DeltaStructField fld(String name, DeltaDataType type, Object id, Object phys) {
    return fld(name, type, true, id, phys);
  }

  /**
   * A field with the given column-mapping id and physical name. A null {@code id} or {@code phys}
   * leaves that key out; {@link #JSON_NULL} sets it to null.
   */
  private static DeltaStructField fld(
      String name, DeltaDataType type, boolean nullable, Object id, Object phys) {
    DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
    if (id != null) {
      meta.put(TableProperties.COLUMN_MAPPING_ID, id == JSON_NULL ? null : id);
    }
    if (phys != null) {
      meta.put(TableProperties.COLUMN_MAPPING_PHYSICAL_NAME, phys == JSON_NULL ? null : phys);
    }
    return new DeltaStructField().name(name).type(type).nullable(nullable).metadata(meta);
  }

  private static DeltaPrimitiveType prim(String type) {
    return new DeltaPrimitiveType().type(type);
  }

  private static DeltaStructType struct(DeltaStructField... fields) {
    return new DeltaStructType().type("struct").fields(List.of(fields));
  }

  private static DeltaArrayType array(DeltaDataType element) {
    return new DeltaArrayType().type("array").elementType(element).containsNull(true);
  }

  private static DeltaMapType map(DeltaDataType key, DeltaDataType value) {
    return new DeltaMapType().type("map").keyType(key).valueType(value).valueContainsNull(true);
  }

  private static DeltaSetSchemaUpdate setColumns(DeltaStructField... fields) {
    return new DeltaSetSchemaUpdate().columns(struct(fields));
  }

  /** Sets {@code delta.columnMapping.mode} in the same request. */
  private static DeltaSetPropertiesUpdate cmMode(String mode) {
    return new DeltaSetPropertiesUpdate()
        .updates(Map.of(TableProperties.COLUMN_MAPPING_MODE, mode));
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
