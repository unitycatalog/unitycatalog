package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaClusteringDomainMetadata;
import io.unitycatalog.client.delta.model.DeltaCreateStagingTableRequest;
import io.unitycatalog.client.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.client.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaPrimitiveType;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.DeltaRowTrackingDomainMetadata;
import io.unitycatalog.client.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.client.delta.model.DeltaStructField;
import io.unitycatalog.client.delta.model.DeltaStructFieldMetadata;
import io.unitycatalog.client.delta.model.DeltaStructType;
import io.unitycatalog.client.delta.model.DeltaTableType;
import io.unitycatalog.client.delta.model.DeltaUniformMetadata;
import io.unitycatalog.client.delta.model.DeltaUniformMetadataIceberg;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.service.delta.DeltaConsts;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.service.delta.UcManagedDeltaContract;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Integration tests for the UC Delta API {@code POST /v1/.../tables} endpoint. Consolidated into
 * one test with sections so the server start + mock-cloud setup runs once. Covers both MANAGED
 * (staging-finalize) and EXTERNAL flows plus the protocol / domain-metadata validation rules.
 */
public class SdkCreateTableTest extends BaseCRUDTestWithMockCredentials {

  private DeltaTablesApi deltaTablesApi;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    deltaTablesApi = new DeltaTablesApi(TestUtils.createApiClient(serverConfig));
    createS3Catalog();
  }

  @Test
  public void testCreateTableEndpoint() throws ApiException {
    // -------- MANAGED happy path: staging -> createTable -> DeltaLoadTableResponse --------
    String tableName = "tbl_happy";
    DeltaStagingTableResponse staging = createStaging(tableName);

    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            managedTableRequest(tableName, staging));

    assertThat(resp.getMetadata()).isNotNull();
    assertThat(resp.getMetadata().getTableType()).isEqualTo(DeltaTableType.MANAGED);
    // Finalized table inherits the staging location and the UUID allocated at staging time.
    assertThat(resp.getMetadata().getLocation()).isEqualTo(staging.getLocation());
    assertThat(resp.getMetadata().getTableUuid()).isEqualTo(staging.getTableId());
    assertThat(resp.getMetadata().getColumns().getFields())
        .extracting(DeltaStructField::getName)
        .containsExactly("id", "amount");
    // Every feature in the request's protocol is mirrored as delta.feature.* = supported in the
    // stored table properties (both reader- and writer-side features collapse to one key per
    // feature name). Client-supplied properties from the request are preserved alongside.
    //
    // The catalogManaged and clusteringColumns entries also pin the server-derived-wins
    // precedence end-to-end. The request's client properties include
    // "delta.feature.catalogManaged" = "client-override" and "delta.clusteringColumns" =
    // "[[\"wrong\"]]"; the assertions below verify the protocol-derived "supported" and the
    // domainMetadata-derived JSON encoding of clusteringColumns=[["id"]] override them.
    assertThat(resp.getMetadata().getProperties())
        .containsEntry(featureKey(TableFeature.CATALOG_MANAGED.specName()), "supported")
        .containsEntry(featureKey(TableFeature.CLUSTERING.specName()), "supported")
        .containsEntry(featureKey(TableFeature.DELETION_VECTORS.specName()), "supported")
        .containsEntry(featureKey(TableFeature.IN_COMMIT_TIMESTAMP.specName()), "supported")
        .containsEntry(featureKey(TableFeature.V2_CHECKPOINT.specName()), "supported")
        .containsEntry(featureKey(TableFeature.VACUUM_PROTOCOL_CHECK.specName()), "supported")
        .containsEntry(TableProperties.CLUSTERING_COLUMNS, "[[\"id\"]]")
        .containsEntry("delta.enableDeletionVectors", "true");

    // -------- EXTERNAL happy path at a fresh (unregistered) storage path --------
    String externalName = "tbl_external";
    String externalLocation = "s3://test-bucket0/external-path/tbl_external";
    DeltaLoadTableResponse extResp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            externalTableRequest(externalName, externalLocation));
    assertThat(extResp.getMetadata().getTableType()).isEqualTo(DeltaTableType.EXTERNAL);
    assertThat(extResp.getMetadata().getLocation()).isEqualTo(externalLocation);

    // -------- name missing --------
    assertDeltaInvalidParam(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest(null, "s3://test-bucket0/unused")),
        "Table name is required");

    // -------- protocol missing --------
    assertDeltaInvalidParam(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_no_protocol", "s3://test-bucket0/unused").protocol(null)),
        "protocol is required");

    // -------- MANAGED without catalogManaged writer feature rejected --------
    DeltaStagingTableResponse stagingNoCm = createStaging("tbl_no_cm");
    assertDeltaInvalidParam(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_no_cm", stagingNoCm)
                    .protocol(
                        new DeltaProtocol()
                            .minReaderVersion(3)
                            .minWriterVersion(7)
                            .readerFeatures(List.of(TableFeature.DELETION_VECTORS.specName()))
                            // catalogManaged intentionally omitted.
                            .writerFeatures(List.of(TableFeature.DELETION_VECTORS.specName())))),
        TableFeature.CATALOG_MANAGED.specName());

    // -------- domain-metadata without matching feature rejected --------
    DeltaStagingTableResponse stagingDm = createStaging("tbl_bad_domain");
    assertDeltaInvalidParam(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_bad_domain", stagingDm)
                    .domainMetadata(
                        new DeltaDomainMetadataUpdates()
                            .deltaRowTracking(
                                new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(100L)))),
        "'rowTracking' writer feature");

    // -------- partition-columns referencing unknown column --------
    DeltaStagingTableResponse stagingForBadPart = createStaging("tbl_bad_part");
    assertDeltaInvalidParam(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_bad_part", stagingForBadPart)
                    .partitionColumns(List.of("nope"))),
        "partition-columns references unknown column: nope");

    // -------- UC_TABLE_ID property doesn't match the staging UUID --------
    // Pins the cross-check at the repository layer: the staging-allocated UUID is the source of
    // truth, and a request claiming a different UUID gets rejected. Without this, a buggy or
    // malicious client could persist an internally-inconsistent UC table (UUID-A persisted, but
    // properties[UC_TABLE_ID]=UUID-B) which downstream commits would only catch much later.
    DeltaStagingTableResponse stagingForWrongId = createStaging("tbl_wrong_id");
    java.util.Map<String, String> wrongIdProps =
        new java.util.HashMap<>(
            fullManagedProperties("00000000-0000-0000-0000-000000000000")); // not the staging UUID
    assertDeltaInvalidParam(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_wrong_id", stagingForWrongId).properties(wrongIdProps)),
        TableProperties.UC_TABLE_ID);

    // -------- partition-columns happy case --------
    DeltaStagingTableResponse stagingPart = createStaging("tbl_part");
    DeltaLoadTableResponse partResp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            managedTableRequest("tbl_part", stagingPart).partitionColumns(List.of("id")));
    assertThat(partResp.getMetadata().getPartitionColumns()).containsExactly("id");

    // -------- uniform happy case: Iceberg sidecar registered at create time --------
    // Exercises the UniForm path: the engine has converted the initial Delta commit to Iceberg
    // and supplies the Iceberg metadata location in the same createTable call. The
    // delta.universalFormat.enabledFormats=iceberg property is the master switch that must
    // match the presence of the uniform block (mirrors the addCommit-time check). The response
    // must round-trip the same uniform block so an Iceberg-REST reader can resolve the table
    // without a follow-up commit.
    DeltaLoadTableResponse uniformResp =
        createTableWithUniform(
            "tbl_uniform",
            s ->
                new DeltaUniformMetadataIceberg()
                    .metadataLocation(s.getLocation() + "/_uniform/iceberg/v1.json")
                    .convertedDeltaVersion(0L)
                    .convertedDeltaTimestamp(1700000000000L));
    assertThat(uniformResp.getUniform()).isNotNull();
    assertThat(uniformResp.getUniform().getIceberg().getMetadataLocation())
        .endsWith("/_uniform/iceberg/v1.json");
    assertThat(uniformResp.getUniform().getIceberg().getConvertedDeltaVersion()).isEqualTo(0L);
    assertThat(uniformResp.getUniform().getIceberg().getConvertedDeltaTimestamp())
        .isEqualTo(1700000000000L);

    // -------- uniform with converted-delta-version=1 (V3 catalog-managed) accepted --------
    // The spec accepts both 0 (V2) and 1 (V3) at create time without committing to which version
    // the table actually is. Pin both so a future regression that hard-codes one is caught.
    DeltaLoadTableResponse v3Resp =
        createTableWithUniform(
            "tbl_uniform_v3",
            s ->
                new DeltaUniformMetadataIceberg()
                    .metadataLocation(s.getLocation() + "/_uniform/v1.json")
                    .convertedDeltaVersion(1L)
                    .convertedDeltaTimestamp(1700000000000L));
    assertThat(v3Resp.getUniform().getIceberg().getConvertedDeltaVersion()).isEqualTo(1L);

    // -------- uniform-enabled property without uniform block rejected --------
    // The property is the master switch. Setting it without supplying the uniform block leaves
    // the table in a state the next addCommit would reject -- catch it at create time.
    DeltaStagingTableResponse stagingPropOnly = createStaging("tbl_uniform_prop_only");
    assertDeltaInvalidParam(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_uniform_prop_only", stagingPropOnly)
                    .properties(uniformEnabledProperties(stagingPropOnly.getTableId().toString()))),
        TableProperties.UNIVERSAL_FORMAT_ENABLED_FORMATS);

    // -------- uniform block without uniform-enabled property rejected --------
    // The inverse: supplying a uniform block without flipping the master switch is also
    // inconsistent. Without this check a table would accept a uniform write at create time
    // while declaring itself NOT UniForm, contradicting the addCommit-time invariant.
    DeltaStagingTableResponse stagingBlockOnly = createStaging("tbl_uniform_block_only");
    assertDeltaInvalidParam(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_uniform_block_only", stagingBlockOnly)
                    .uniform(
                        new DeltaUniformMetadata()
                            .iceberg(
                                new DeltaUniformMetadataIceberg()
                                    .metadataLocation("s3://test-bucket0/iceberg/blk.json")))),
        TableProperties.UNIVERSAL_FORMAT_ENABLED_FORMATS);

    // -------- uniform without iceberg sub-block rejected --------
    DeltaStagingTableResponse stagingNoIce = createStaging("tbl_uniform_no_ice");
    assertDeltaInvalidParam(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_uniform_no_ice", stagingNoIce)
                    .properties(uniformEnabledProperties(stagingNoIce.getTableId().toString()))
                    .uniform(new DeltaUniformMetadata())),
        "uniform.iceberg");

    // -------- uniform.iceberg.metadata-location missing rejected --------
    assertDeltaInvalidParam(
        () -> createTableWithUniform("tbl_uniform_no_loc", s -> new DeltaUniformMetadataIceberg()),
        "metadata-location");

    // -------- uniform.iceberg.converted-delta-version missing rejected --------
    assertDeltaInvalidParam(
        () ->
            createTableWithUniform(
                "tbl_uniform_no_ver",
                s ->
                    new DeltaUniformMetadataIceberg()
                        .metadataLocation(s.getLocation() + "/_uniform/v1.json")
                        .convertedDeltaTimestamp(1700000000000L)),
        "converted-delta-version is required");

    // -------- uniform.iceberg.converted-delta-timestamp missing rejected --------
    assertDeltaInvalidParam(
        () ->
            createTableWithUniform(
                "tbl_uniform_no_ts",
                s ->
                    new DeltaUniformMetadataIceberg()
                        .metadataLocation(s.getLocation() + "/_uniform/v1.json")
                        .convertedDeltaVersion(0L)),
        "converted-delta-timestamp is required");

    // -------- uniform.iceberg.metadata-location not a subpath of table location rejected --------
    // The Iceberg metadata MUST be inside the table's storage root so that table-level credential
    // vending and lifecycle (delete/rename) cover it. A path outside the root would be orphaned
    // when the table is dropped and is rejected at create time.
    assertDeltaInvalidParam(
        () ->
            createTableWithUniform(
                "tbl_uniform_bad_path",
                s ->
                    new DeltaUniformMetadataIceberg()
                        // Sibling location, not a subpath of the staging location.
                        .metadataLocation("s3://test-bucket0/elsewhere/iceberg/v1.json")
                        .convertedDeltaVersion(0L)
                        .convertedDeltaTimestamp(1700000000000L)),
        "must be a subpath");

    // -------- converted-delta-version != 0 or 1 rejected --------
    // At create time the only legal values are 0 (V2 catalog-managed) and 1 (V3). Anything else
    // would imply this createTable call is replaying a later commit, which is not what create is
    // for.
    assertDeltaInvalidParam(
        () ->
            createTableWithUniform(
                "tbl_uniform_bad_ver",
                s ->
                    new DeltaUniformMetadataIceberg()
                        .metadataLocation(s.getLocation() + "/_uniform/v1.json")
                        .convertedDeltaVersion(5L)
                        .convertedDeltaTimestamp(1700000000000L)),
        "must be 0 or 1");

    // -------- metadata-location oversized rejected --------
    // The DAO column is bounded; reject at the API boundary so a request that "succeeds" through
    // the validator can never fail at persist time. Constructed by padding a query-string suffix
    // onto a valid subpath so the path-shape rule still passes and we exercise the size check
    // specifically.
    String oversizedSuffix = "?pad=" + "x".repeat(70_000);
    assertDeltaInvalidParam(
        () ->
            createTableWithUniform(
                "tbl_uniform_big_loc",
                s ->
                    new DeltaUniformMetadataIceberg()
                        .metadataLocation(s.getLocation() + "/_uniform/v1.json" + oversizedSuffix)
                        .convertedDeltaVersion(0L)
                        .convertedDeltaTimestamp(1700000000000L)),
        "exceeds the maximum allowed");

    // -------- base-converted-delta-version supplied at create time rejected --------
    // base-converted-delta-version is the sequential-validation hook for incremental conversion
    // commits; at create time there is no prior stored converted-delta-version for it to match
    // against, so supplying it is always wrong.
    assertDeltaInvalidParam(
        () ->
            createTableWithUniform(
                "tbl_uniform_with_base",
                s ->
                    new DeltaUniformMetadataIceberg()
                        .metadataLocation(s.getLocation() + "/_uniform/v1.json")
                        .convertedDeltaVersion(0L)
                        .convertedDeltaTimestamp(1700000000000L)
                        .baseConvertedDeltaVersion(0L)),
        "base-converted-delta-version must not be set at create time");
  }

  /**
   * Full UC-managed properties augmented with the UniForm enabled-formats property so requests that
   * supply a {@code uniform} block satisfy the create-time consistency check.
   */
  private static Map<String, String> uniformEnabledProperties(String tableId) {
    Map<String, String> props = new java.util.HashMap<>(fullManagedProperties(tableId));
    props.put(
        TableProperties.UNIVERSAL_FORMAT_ENABLED_FORMATS, DeltaConsts.UNIVERSAL_FORMAT_ICEBERG);
    return props;
  }

  /** Creates a catalog + schema whose staging tables resolve under s3://test-bucket0/. */
  @SneakyThrows
  private void createS3Catalog() {
    catalogOperations.createCatalog(
        new CreateCatalog()
            .name(TestUtils.CATALOG_NAME2)
            .storageRoot("s3://test-bucket0/catalogs/delta-api"));
    schemaOperations.createSchema(
        new CreateSchema().name(TestUtils.SCHEMA_NAME2).catalogName(TestUtils.CATALOG_NAME2));
  }

  /** Canonical (id long, amount double) columns shared across requests. */
  private static DeltaStructType simpleSchema() {
    return new DeltaStructType()
        .type("struct")
        .fields(
            List.of(
                new DeltaStructField()
                    .name("id")
                    .type(new DeltaPrimitiveType().type("long"))
                    .nullable(false)
                    .metadata(new DeltaStructFieldMetadata()),
                new DeltaStructField()
                    .name("amount")
                    .type(new DeltaPrimitiveType().type("double"))
                    .nullable(true)
                    .metadata(new DeltaStructFieldMetadata())));
  }

  /**
   * Full UC catalog-managed protocol: every required feature in the right list, plus CLUSTERING in
   * writerFeatures so the canonical request can carry a {@code deltaClustering} domain-metadata
   * block (see {@link #managedTableRequest(String, DeltaStagingTableResponse)}).
   */
  private static DeltaProtocol managedProtocol() {
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
                TableFeature.CLUSTERING.specName(),
                TableFeature.DELETION_VECTORS.specName(),
                TableFeature.IN_COMMIT_TIMESTAMP.specName(),
                TableFeature.V2_CHECKPOINT.specName(),
                TableFeature.VACUUM_PROTOCOL_CHECK.specName()));
  }

  /**
   * Properties that satisfy the UC-managed contract (the staging response advertises these). The
   * engine-generated values can be any non-null placeholder; UC just checks presence.
   */
  private static Map<String, String> fullManagedProperties(String tableId) {
    Map<String, String> props =
        new java.util.HashMap<>(UcManagedDeltaContract.REQUIRED_FIXED_PROPERTIES);
    props.put(TableProperties.UC_TABLE_ID, tableId);
    // User-specified properties under server-derived keys are overridden by the structured
    // protocol/domain-metadata blocks. End-to-end override is pinned by the assertions on
    // featureKey(CATALOG_MANAGED) and CLUSTERING_COLUMNS in testCreateTableEndpoint.
    //   - delta.feature.catalogManaged: protocol-derived (CATALOG_MANAGED is in writerFeatures);
    //     "client-override" loses to the derived "supported".
    //   - delta.clusteringColumns: domain-metadata-derived (managedTableRequest carries a
    //     deltaClustering block with clusteringColumns=[["id"]]); "[[\"wrong\"]]" loses to the
    //     derived JSON encoding of the structured block.
    props.put(
        TableProperties.FEATURE_PREFIX + TableFeature.CATALOG_MANAGED.specName(),
        "client-override");
    props.put(TableProperties.CLUSTERING_COLUMNS, "[[\"wrong\"]]");
    return props;
  }

  /** Build a canonical MANAGED Delta table request bound to a freshly-allocated staging table. */
  private static DeltaCreateTableRequest managedTableRequest(
      String name, DeltaStagingTableResponse staging) {
    return new DeltaCreateTableRequest()
        .name(name)
        .location(staging.getLocation())
        .tableType(DeltaTableType.MANAGED)
        .columns(simpleSchema())
        .protocol(managedProtocol())
        .domainMetadata(
            new DeltaDomainMetadataUpdates()
                .deltaClustering(
                    new DeltaClusteringDomainMetadata().clusteringColumns(List.of(List.of("id")))))
        .properties(fullManagedProperties(staging.getTableId().toString()))
        .lastCommitTimestampMs(1700000000000L);
  }

  /**
   * Build a MANAGED request not tied to a staging response -- for tests that exercise pre-contract
   * failure paths (missing required field, wrong format) where the staging UUID never gets read.
   */
  private static DeltaCreateTableRequest managedTableRequest(String name, String location) {
    return new DeltaCreateTableRequest()
        .name(name)
        .location(location)
        .tableType(DeltaTableType.MANAGED)
        .columns(simpleSchema())
        .protocol(managedProtocol())
        .properties(fullManagedProperties("00000000-0000-0000-0000-000000000000"))
        .lastCommitTimestampMs(1700000000000L);
  }

  /** Build an EXTERNAL Delta table request at an arbitrary storage path. */
  private static DeltaCreateTableRequest externalTableRequest(String name, String location) {
    return new DeltaCreateTableRequest()
        .name(name)
        .location(location)
        .tableType(DeltaTableType.EXTERNAL)
        .columns(simpleSchema())
        // EXTERNAL tables don't require catalogManaged; use a minimal modern Delta protocol.
        .protocol(
            new DeltaProtocol()
                .minReaderVersion(3)
                .minWriterVersion(7)
                .readerFeatures(List.of(TableFeature.DELETION_VECTORS.specName()))
                .writerFeatures(List.of(TableFeature.DELETION_VECTORS.specName())))
        .properties(Map.of("delta.enableDeletionVectors", "true"))
        .lastCommitTimestampMs(1700000000000L);
  }

  /** {@code delta.feature.<name>} for the stored UC property assertions. */
  private static String featureKey(String feature) {
    return TableProperties.FEATURE_PREFIX + feature;
  }

  /**
   * Shorthand for the {@link DeltaErrorType#INVALID_PARAMETER_VALUE_EXCEPTION} pattern, which is
   * the only error type the negative cases in this suite assert against.
   */
  private static void assertDeltaInvalidParam(
      Executable executable, String expectedMessageSubstring) {
    TestUtils.assertDeltaApiException(
        executable, DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION, expectedMessageSubstring);
  }

  /** Allocate a fresh managed staging table under {@code CATALOG_NAME2.SCHEMA_NAME2}. */
  private DeltaStagingTableResponse createStaging(String name) throws ApiException {
    return deltaTablesApi.createStagingTable(
        TestUtils.CATALOG_NAME2,
        TestUtils.SCHEMA_NAME2,
        new DeltaCreateStagingTableRequest().name(name));
  }

  /**
   * Stage + finalize a managed Delta table whose request carries the uniform-enabled property and a
   * uniform-iceberg block built from the staging response (so callers can compute {@code
   * metadata-location} as a subpath of the staging location). For negative cases pass an iceberg
   * builder that produces an invalid block; the call site wraps this in {@code
   * assertDeltaInvalidParam}.
   */
  private DeltaLoadTableResponse createTableWithUniform(
      String name, Function<DeltaStagingTableResponse, DeltaUniformMetadataIceberg> icebergBuilder)
      throws ApiException {
    DeltaStagingTableResponse staging = createStaging(name);
    return deltaTablesApi.createTable(
        TestUtils.CATALOG_NAME2,
        TestUtils.SCHEMA_NAME2,
        managedTableRequest(name, staging)
            .properties(uniformEnabledProperties(staging.getTableId().toString()))
            .uniform(new DeltaUniformMetadata().iceberg(icebergBuilder.apply(staging))));
  }
}
