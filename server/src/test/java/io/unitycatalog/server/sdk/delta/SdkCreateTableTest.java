package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.ClusteringDomainMetadata;
import io.unitycatalog.client.delta.model.CreateStagingTableRequest;
import io.unitycatalog.client.delta.model.CreateTableRequest;
import io.unitycatalog.client.delta.model.DataSourceFormat;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.DomainMetadataUpdates;
import io.unitycatalog.client.delta.model.ErrorType;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.PrimitiveType;
import io.unitycatalog.client.delta.model.StagingTableResponse;
import io.unitycatalog.client.delta.model.StructField;
import io.unitycatalog.client.delta.model.StructFieldMetadata;
import io.unitycatalog.client.delta.model.StructType;
import io.unitycatalog.client.delta.model.TableType;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the Delta REST Catalog {@code POST /v1/.../tables} endpoint. Consolidated
 * into one test with sections so the server start + mock-cloud setup runs once. Covers both MANAGED
 * (staging-finalize) and EXTERNAL flows plus the protocol / domain-metadata validation rules.
 */
public class SdkCreateTableTest extends BaseCRUDTestWithMockCredentials {

  private TablesApi deltaTablesApi;

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
    deltaTablesApi = new TablesApi(TestUtils.createApiClient(serverConfig));
    createS3Catalog();
  }

  @Test
  public void testCreateTableEndpoint() throws ApiException {
    // -------- MANAGED happy path: staging -> createTable -> LoadTableResponse --------
    String tableName = "tbl_happy";
    StagingTableResponse staging =
        deltaTablesApi.createStagingTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            new CreateStagingTableRequest().name(tableName));

    LoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            managedTableRequest(tableName, staging));

    assertThat(resp.getMetadata()).isNotNull();
    assertThat(resp.getMetadata().getTableType()).isEqualTo(TableType.MANAGED);
    // Finalized table inherits the staging location and the UUID allocated at staging time.
    assertThat(resp.getMetadata().getLocation()).isEqualTo(staging.getLocation());
    assertThat(resp.getMetadata().getTableUuid()).isEqualTo(staging.getTableId());
    assertThat(resp.getMetadata().getColumns().getFields())
        .extracting(StructField::getName)
        .containsExactly("id", "amount");
    // Every feature in the request's protocol is mirrored as delta.feature.* = supported in the
    // stored table properties (both reader- and writer-side features collapse to one key per
    // feature name). Client-supplied properties from the request are preserved alongside.
    assertThat(resp.getMetadata().getProperties())
        .containsEntry(featureKey(TableFeature.CATALOG_MANAGED.specName()), "supported")
        .containsEntry(featureKey(TableFeature.DELETION_VECTORS.specName()), "supported")
        .containsEntry(featureKey(TableFeature.IN_COMMIT_TIMESTAMP.specName()), "supported")
        .containsEntry(featureKey(TableFeature.V2_CHECKPOINT.specName()), "supported")
        .containsEntry(featureKey(TableFeature.VACUUM_PROTOCOL_CHECK.specName()), "supported")
        .containsEntry("delta.enableDeletionVectors", "true");

    // -------- EXTERNAL happy path at a fresh (unregistered) storage path --------
    String externalName = "tbl_external";
    String externalLocation = "s3://test-bucket0/external-path/tbl_external";
    LoadTableResponse extResp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            externalTableRequest(externalName, externalLocation));
    assertThat(extResp.getMetadata().getTableType()).isEqualTo(TableType.EXTERNAL);
    assertThat(extResp.getMetadata().getLocation()).isEqualTo(externalLocation);

    // -------- ICEBERG rejected --------
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_iceberg", "s3://test-bucket0/unused")
                    .dataSourceFormat(DataSourceFormat.ICEBERG)),
        ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "Unsupported data-source-format");

    // -------- name missing --------
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest(null, "s3://test-bucket0/unused")),
        ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "Table name is required");

    // -------- protocol missing --------
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_no_protocol", "s3://test-bucket0/unused").protocol(null)),
        ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "protocol is required");

    // -------- MANAGED without catalogManaged writer feature rejected --------
    StagingTableResponse stagingNoCm =
        deltaTablesApi.createStagingTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            new CreateStagingTableRequest().name("tbl_no_cm"));
    TestUtils.assertDeltaApiException(
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
        ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        TableFeature.CATALOG_MANAGED.specName());

    // -------- domain-metadata without matching feature rejected --------
    StagingTableResponse stagingDm =
        deltaTablesApi.createStagingTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            new CreateStagingTableRequest().name("tbl_bad_domain"));
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_bad_domain", stagingDm)
                    .domainMetadata(
                        new DomainMetadataUpdates()
                            .deltaClustering(
                                new ClusteringDomainMetadata()
                                    .clusteringColumns(List.of(List.of("id")))))),
        ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "'clustering' writer feature");

    // -------- partition-columns referencing unknown column --------
    StagingTableResponse stagingForBadPart =
        deltaTablesApi.createStagingTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            new CreateStagingTableRequest().name("tbl_bad_part"));
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_bad_part", stagingForBadPart)
                    .partitionColumns(List.of("nope"))),
        ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "partition-columns references unknown column: nope");

    // -------- UC_TABLE_ID property doesn't match the staging UUID --------
    // Pins the cross-check at the repository layer: the staging-allocated UUID is the source of
    // truth, and a request claiming a different UUID gets rejected. Without this, a buggy or
    // malicious client could persist an internally-inconsistent UC table (UUID-A persisted, but
    // properties[UC_TABLE_ID]=UUID-B) which downstream commits would only catch much later.
    StagingTableResponse stagingForWrongId =
        deltaTablesApi.createStagingTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            new CreateStagingTableRequest().name("tbl_wrong_id"));
    java.util.Map<String, String> wrongIdProps =
        new java.util.HashMap<>(
            fullManagedProperties("00000000-0000-0000-0000-000000000000")); // not the staging UUID
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.createTable(
                TestUtils.CATALOG_NAME2,
                TestUtils.SCHEMA_NAME2,
                managedTableRequest("tbl_wrong_id", stagingForWrongId).properties(wrongIdProps)),
        ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        TableProperties.UC_TABLE_ID);

    // -------- partition-columns happy case --------
    StagingTableResponse stagingPart =
        deltaTablesApi.createStagingTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            new CreateStagingTableRequest().name("tbl_part"));
    LoadTableResponse partResp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME2,
            TestUtils.SCHEMA_NAME2,
            managedTableRequest("tbl_part", stagingPart).partitionColumns(List.of("id")));
    assertThat(partResp.getMetadata().getPartitionColumns()).containsExactly("id");
  }

  /** Creates a catalog + schema whose staging tables resolve under s3://test-bucket0/. */
  @SneakyThrows
  private void createS3Catalog() {
    catalogOperations.createCatalog(
        new CreateCatalog()
            .name(TestUtils.CATALOG_NAME2)
            .storageRoot("s3://test-bucket0/catalogs/drc"));
    schemaOperations.createSchema(
        new CreateSchema().name(TestUtils.SCHEMA_NAME2).catalogName(TestUtils.CATALOG_NAME2));
  }

  /** Canonical (id long, amount double) columns shared across requests. */
  private static StructType simpleSchema() {
    return new StructType()
        .type("struct")
        .fields(
            List.of(
                new StructField()
                    .name("id")
                    .type(new PrimitiveType().type("long"))
                    .nullable(false)
                    .metadata(new StructFieldMetadata()),
                new StructField()
                    .name("amount")
                    .type(new PrimitiveType().type("double"))
                    .nullable(true)
                    .metadata(new StructFieldMetadata())));
  }

  /** Full UC catalog-managed protocol: every required feature in the right list. */
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
    Map<String, String> props = new java.util.HashMap<>();
    props.put(TableProperties.CHECKPOINT_POLICY, "v2");
    props.put(TableProperties.ENABLE_DELETION_VECTORS, "true");
    props.put(TableProperties.ENABLE_IN_COMMIT_TIMESTAMPS, "true");
    props.put(TableProperties.UC_TABLE_ID, tableId);
    props.put(TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, "0");
    props.put(TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP, "1700000000000");
    return props;
  }

  /** Build a canonical MANAGED Delta table request bound to a freshly-allocated staging table. */
  private static CreateTableRequest managedTableRequest(String name, StagingTableResponse staging) {
    return new CreateTableRequest()
        .name(name)
        .location(staging.getLocation())
        .tableType(TableType.MANAGED)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .columns(simpleSchema())
        .protocol(managedProtocol())
        .properties(fullManagedProperties(staging.getTableId().toString()));
  }

  /**
   * Build a MANAGED request not tied to a staging response -- for tests that exercise pre-contract
   * failure paths (missing required field, wrong format) where the staging UUID never gets read.
   */
  private static CreateTableRequest managedTableRequest(String name, String location) {
    return new CreateTableRequest()
        .name(name)
        .location(location)
        .tableType(TableType.MANAGED)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .columns(simpleSchema())
        .protocol(managedProtocol())
        .properties(fullManagedProperties("00000000-0000-0000-0000-000000000000"));
  }

  /** Build an EXTERNAL Delta table request at an arbitrary storage path. */
  private static CreateTableRequest externalTableRequest(String name, String location) {
    return new CreateTableRequest()
        .name(name)
        .location(location)
        .tableType(TableType.EXTERNAL)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .columns(simpleSchema())
        // EXTERNAL tables don't require catalogManaged; use a minimal modern Delta protocol.
        .protocol(
            new DeltaProtocol()
                .minReaderVersion(3)
                .minWriterVersion(7)
                .readerFeatures(List.of(TableFeature.DELETION_VECTORS.specName()))
                .writerFeatures(List.of(TableFeature.DELETION_VECTORS.specName())))
        .properties(Map.of("delta.enableDeletionVectors", "true"));
  }

  /** {@code delta.feature.<name>} for the stored UC property assertions. */
  private static String featureKey(String feature) {
    return TableProperties.FEATURE_PREFIX + feature;
  }
}
