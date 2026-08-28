package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.LocalMappingFileOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.metrics.CommitMetrics;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for the Iceberg REST catalog service, driven through the hand-rolled {@link
 * IcebergRestClient}. Extends {@link BaseCRUDTestWithMockCredentials} so the server is wired with
 * mock cloud-credential vendors and a default catalog + schema ({@link TestUtils#CATALOG_NAME} /
 * {@link TestUtils#SCHEMA_NAME}) already exist; tests use those directly and create additional
 * namespaces only where the scenario is about namespace creation itself.
 */
public class IcebergRestCatalogTest extends BaseCRUDTestWithMockCredentials {

  // Base path the Iceberg REST catalog is mounted at; used by the two raw probes for HTTP-level
  // checks the typed client can't express: a missing warehouse param and a body that isn't a valid
  // request. Every other call goes through IcebergRestClient.
  private static final String ICEBERG_PREFIX = "/api/2.1/unity-catalog/iceberg";
  private static final int PAGE_SIZE = PagedListingHelper.DEFAULT_PAGE_SIZE;

  @TempDir private Path icebergTableLocation;

  private TableOperations tableOperations;
  private IcebergRestClient icebergClient;
  // The decorator that maps a registered cloud prefix to a local dir; kept so a test can register
  // its catalog root and resolve cloud locations back to local paths.
  private LocalMappingFileOperations mappingFileOperations;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // Native Iceberg REST writes are opt-in in production; this integration suite exercises them.
    serverProperties.setProperty(Property.ICEBERG_TABLE_ENABLED.getKey(), "true");
  }

  @Override
  protected FileOperations decorateFileOperations(FileOperations fileOperations) {
    // Map registered cloud prefixes to local files so the cloud file-IO path can run without a real
    // backend, validating UC vended the expected credentials before any access. Roots are
    // registered after the server starts (setUp / the per-cloud test), so keep the instance.
    mappingFileOperations =
        new LocalMappingFileOperations(fileOperations, EXPECTED_VENDED_S3_CREDENTIALS);
    return mappingFileOperations;
  }

  @BeforeEach
  @Override
  public void setUp() {
    // Creates the default (file://) catalog + schema and the mock cloud-credential vendors.
    super.setUp();
    tableOperations = new SdkTableOperations(TestUtils.createApiClient(serverConfig));
    icebergClient = new IcebergRestClient(serverConfig);
  }

  /**
   * Registers a cloud-storage mapping and creates a catalog rooted there with the {@code
   * SCHEMA_NAME} namespace the table tests use. Namespace create/conflict coverage lives in
   * testNamespaces.
   */
  @SneakyThrows
  private void createCloudCatalog(String catalog, String cloudRoot, Path localDir) {
    mappingFileOperations.mapLocation(NormalizedURL.from(cloudRoot), localDir);
    catalogOperations.createCatalog(new CreateCatalog().name(catalog).storageRoot(cloudRoot));
    icebergClient.createNamespace(catalog, TestUtils.SCHEMA_NAME);
  }

  @Test
  public void testConfig() throws Exception {
    // Successful config with the warehouse param carries the catalog prefix override and the full
    // endpoint list (writes included, since ICEBERG_TABLE_ENABLED is on for this suite).
    ConfigResponse resp = icebergClient.config(TestUtils.CATALOG_NAME);
    assertThat(resp.overrides()).containsEntry("prefix", "catalogs/" + TestUtils.CATALOG_NAME);
    assertThat(resp.endpoints())
        .containsExactlyInAnyOrder(
            Endpoint.V1_LIST_NAMESPACES,
            Endpoint.V1_LOAD_NAMESPACE,
            Endpoint.V1_TABLE_EXISTS,
            Endpoint.V1_LOAD_TABLE,
            Endpoint.V1_LOAD_VIEW,
            Endpoint.V1_REPORT_METRICS,
            Endpoint.V1_LIST_TABLES,
            Endpoint.V1_CREATE_NAMESPACE,
            Endpoint.V1_CREATE_TABLE,
            Endpoint.V1_UPDATE_TABLE,
            Endpoint.V1_DELETE_TABLE);

    // Not setting the warehouse param should result in a 400 BadRequestException.
    HttpResponse<String> missingWarehouse =
        TestUtils.sendRaw(serverConfig, "GET", ICEBERG_PREFIX + "/v1/config", Optional.empty());
    assertThat(missingWarehouse.statusCode()).isEqualTo(400);
    assertThat(ErrorResponseParser.fromJson(missingWarehouse.body()).type())
        .isEqualTo(BadRequestException.class.getSimpleName());
  }

  @Test
  public void testNamespaces() throws Exception {
    // The default SCHEMA_NAME namespace has no properties; create a second one with properties to
    // verify that schema properties flow through create and getNamespace.
    CreateNamespaceRequest createRequest =
        CreateNamespaceRequest.builder()
            .withNamespace(Namespace.of(TestUtils.SCHEMA_NAME2))
            .setProperties(TestUtils.PROPERTIES)
            .build();
    CreateNamespaceResponse created =
        icebergClient.createNamespace(TestUtils.CATALOG_NAME, createRequest);
    assertThat(created.namespace()).isEqualTo(Namespace.of(TestUtils.SCHEMA_NAME2));
    assertThat(created.properties()).isEqualTo(TestUtils.PROPERTIES);

    // creating it again is a 409 conflict
    TestUtils.assertIcebergApiException(
        () -> icebergClient.createNamespace(TestUtils.CATALOG_NAME, createRequest),
        409,
        "already exists");

    // GetNamespace
    {
      GetNamespaceResponse namespace =
          icebergClient.loadNamespace(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME2);
      assertThat(namespace.namespace()).isEqualTo(Namespace.of(TestUtils.SCHEMA_NAME2));
      assertThat(namespace.properties()).isEqualTo(TestUtils.PROPERTIES);

      // loading a namespace that doesn't exist is a 404
      TestUtils.assertIcebergApiException(
          () -> icebergClient.loadNamespace(TestUtils.CATALOG_NAME, "nonexistent_namespace"), 404);
    }

    // ListNamespaces
    {
      ListNamespacesResponse namespaces = icebergClient.listNamespaces(TestUtils.CATALOG_NAME);
      assertThat(namespaces.namespaces())
          .containsExactlyInAnyOrder(
              Namespace.of(TestUtils.SCHEMA_NAME), Namespace.of(TestUtils.SCHEMA_NAME2));
    }
  }

  @Test
  public void testTable() throws Exception {
    ColumnInfo columnInfo1 =
        new ColumnInfo()
            .name("as_int")
            .typeText("INTEGER")
            .typeJson(
                "{\"name\":\"as_int\",\"type\":\"integer\"," + "\"nullable\":true,\"metadata\":{}}")
            .typeName(ColumnTypeName.INT)
            .typePrecision(10)
            .typeScale(0)
            .position(0)
            .comment("Integer column")
            .nullable(true);
    ColumnInfo columnInfo2 =
        new ColumnInfo()
            .name("as_string")
            .typeText("VARCHAR(255)")
            .typeJson(
                "{\"name\":\"as_string\",\"type\":\"string\","
                    + "\"nullable\":true,\"metadata\":{}}")
            .typeName(ColumnTypeName.STRING)
            .position(1)
            .comment("String column")
            .nullable(true);
    CreateTable createTableRequest =
        new CreateTable()
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(List.of(columnInfo1, columnInfo2))
            .comment(TestUtils.COMMENT)
            // Placeholder external location; the DAO url is repointed at the temp table root below.
            .storageLocation(testDirectoryRoot.resolve("staging").toString())
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA);
    TableInfo tableInfo = tableOperations.createTable(createTableRequest);

    // Uniform table doesn't exist at this point
    assertThat(
            icebergClient.tableExists(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME))
        .isFalse();
    TestUtils.assertIcebergApiException(
        () ->
            icebergClient.loadTable(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME),
        404,
        "does not exist");

    // Register UniForm-derived Iceberg metadata for the table. The fixture's baked table root is
    // rewritten onto a hermetic temp directory and the metadata file is written under it, modeling
    // a real UniForm table whose persisted metadata pointer lives inside the table location that
    // the REST load path validates.
    Path tableRoot = testDirectoryRoot.resolve("uniform_iceberg_table");
    NormalizedURL tableLocation = NormalizedURL.from(tableRoot.toUri());
    Path metadataFile = tableRoot.resolve("metadata/v1.metadata.json");
    Files.createDirectories(metadataFile.getParent());
    try (InputStream fixture =
        Objects.requireNonNull(this.getClass().getResourceAsStream("/iceberg.metadata.json"))) {
      String fixtureJson =
          new String(fixture.readAllBytes(), StandardCharsets.UTF_8)
              .replace("file:/tmp/uniform_iceberg_table", tableLocation.toString());
      Files.writeString(metadataFile, fixtureJson);
    }
    String metadataLocation = metadataFile.toUri().toString();
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      Transaction tx = session.beginTransaction();
      TableInfoDAO tableInfoDAO = TableInfoDAO.builder().build();
      assertThat(tableInfo.getTableId()).isNotNull();
      session.load(tableInfoDAO, UUID.fromString(tableInfo.getTableId()));
      tableInfoDAO.setUrl(tableLocation.toString());
      tableInfoDAO.setUniformIcebergMetadataLocation(metadataLocation);
      session.merge(tableInfoDAO);
      tx.commit();
    }

    // Now the uniform table exists
    assertThat(
            icebergClient.tableExists(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME))
        .isTrue();

    // metadata is valid metadata content and metadata location matches
    {
      LoadTableResponse loadTableResponse =
          icebergClient.loadTable(
              TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .isEqualTo(metadataFile.toString());
    }

    // List uniform tables
    {
      ListTablesResponse listTablesResponse =
          icebergClient.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
      assertThat(listTablesResponse.identifiers())
          .containsExactly(TableIdentifier.of(TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME));
    }

    // UniForm-derived Iceberg metadata is read-only: commits and drops through the Iceberg REST
    // catalog must be rejected.
    {
      UpdateTableRequest commitRequest =
          new UpdateTableRequest(
              List.of(), List.of(new MetadataUpdate.SetProperties(Map.of("foo", "bar"))));
      TestUtils.assertIcebergApiException(
          () ->
              icebergClient.updateTable(
                  TestUtils.CATALOG_NAME,
                  TestUtils.SCHEMA_NAME,
                  TestUtils.TABLE_NAME,
                  commitRequest),
          400);
      TestUtils.assertIcebergApiException(
          () ->
              icebergClient.dropTable(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME),
          400);
    }

    // Credentials must never be scoped by a conflicting location in the metadata payload. Repoint
    // the persisted location away from the metadata's table root and the load must be rejected
    // rather than vend credentials for a mismatched location.
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      Transaction tx = session.beginTransaction();
      TableInfoDAO conflicting =
          session.get(TableInfoDAO.class, UUID.fromString(tableInfo.getTableId()));
      assertThat(conflicting).isNotNull();
      conflicting.setUrl(icebergTableLocation.resolve("other_table").toString());
      tx.commit();
    }
    TestUtils.assertIcebergApiException(
        () ->
            icebergClient.loadTable(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME),
        400,
        "persisted table location");
  }

  @Test
  public void testIcebergTableWriteLifecycle() throws Exception {
    // Runs the full create/commit/load/drop lifecycle against an s3:// mock-storage catalog, so
    // this single test exercises the cloud file-IO path and credential vending as well as the CRUD
    // semantics. The s3:// key is the absolute temp dir, which maps back onto that local dir.
    String catalog = TestUtils.CATALOG_NAME2;
    String namespace = TestUtils.SCHEMA_NAME;
    String tableFullName = catalog + "." + namespace + "." + TestUtils.TABLE_NAME;
    String s3CatalogRoot =
        "s3://" + CONFIGURED_BUCKET + testDirectoryRoot.toAbsolutePath() + "/s3catalog";
    createCloudCatalog(
        catalog, s3CatalogRoot, testDirectoryRoot.toAbsolutePath().resolve("s3catalog"));

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    String location = s3CatalogRoot + "/ext_table";

    // Staged creation is stateless: it returns metadata without a metadata-location and
    // registers nothing, so the direct create below still succeeds.
    {
      CreateTableRequest request =
          CreateTableRequest.builder()
              .withName(TestUtils.TABLE_NAME)
              .withSchema(schema)
              .withLocation(location)
              .stageCreate()
              .build();
      LoadTableResponse loadTableResponse = icebergClient.createTable(catalog, namespace, request);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation()).isNull();
      TestUtils.assertIcebergApiException(
          () -> icebergClient.loadTable(catalog, namespace, TestUtils.TABLE_NAME), 404);
    }

    // Create the table
    String initialMetadataLocation;
    {
      CreateTableRequest request =
          CreateTableRequest.builder()
              .withName(TestUtils.TABLE_NAME)
              .withSchema(schema)
              .withLocation(location)
              .setProperty("created-by", "iceberg-rest-test")
              .build();
      LoadTableResponse loadTableResponse = icebergClient.createTable(catalog, namespace, request);
      initialMetadataLocation = loadTableResponse.tableMetadata().metadataFileLocation();
      // The metadata "commit" file was written to the cloud location, i.e. the mapped local path.
      assertThat(initialMetadataLocation)
          .startsWith("s3://" + CONFIGURED_BUCKET)
          .contains("/metadata/00000-");
      assertThat(Files.exists(localPathOf(initialMetadataLocation))).isTrue();
      // Credential vending is real: the vended mock S3 credentials appear in the load config.
      assertThat(loadTableResponse.config())
          .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, S3_ACCESS_KEY)
          .containsEntry(S3FileIOProperties.SESSION_TOKEN, S3_SESSION_TOKEN);
      assertThat(loadTableResponse.tableMetadata().schema().columns()).hasSize(2);
      assertThat(loadTableResponse.tableMetadata().properties())
          .containsEntry("created-by", "iceberg-rest-test");
      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        TableInfoDAO tableInfoDAO = getTableByName(session, TestUtils.TABLE_NAME);
        assertThat(tableInfoDAO.getUniformIcebergMetadataLocation())
            .isEqualTo(NormalizedURL.from(initialMetadataLocation).toString());
      }

      // creating it again is a conflict
      TestUtils.assertIcebergApiException(
          () -> icebergClient.createTable(catalog, namespace, request), 409, "already exists");
    }

    // The table is registered in UC as a native Iceberg table with converted columns
    {
      TableInfo tableInfo = tableOperations.getTable(tableFullName);
      assertThat(tableInfo.getDataSourceFormat()).isEqualTo(DataSourceFormat.ICEBERG);
      assertThat(tableInfo.getTableType()).isEqualTo(TableType.EXTERNAL);
      assertThat(tableInfo.getColumns())
          .extracting(ColumnInfo::getName)
          .containsExactly("id", "data");
      assertThat(tableInfo.getProperties()).containsEntry("created-by", "iceberg-rest-test");
    }

    // The table is loadable and listable through the Iceberg REST catalog
    String tableUuid;
    {
      assertThat(icebergClient.tableExists(catalog, namespace, TestUtils.TABLE_NAME)).isTrue();

      LoadTableResponse loadTableResponse =
          icebergClient.loadTable(catalog, namespace, TestUtils.TABLE_NAME);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .isEqualTo(initialMetadataLocation);
      tableUuid = loadTableResponse.tableMetadata().uuid();

      ListTablesResponse listTablesResponse = icebergClient.listTables(catalog, namespace);
      assertThat(listTablesResponse.identifiers())
          .containsExactly(TableIdentifier.of(namespace, TestUtils.TABLE_NAME));
    }

    // Commit an update against the table
    {
      Schema updatedSchema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()),
              Types.NestedField.optional(3, "category", Types.StringType.get()));
      UpdateTableRequest request =
          new UpdateTableRequest(
              List.of(new UpdateRequirement.AssertTableUUID(tableUuid)),
              List.of(
                  new MetadataUpdate.AddSchema(updatedSchema),
                  new MetadataUpdate.SetCurrentSchema(-1),
                  new MetadataUpdate.SetProperties(Map.of("foo", "bar"))));
      LoadTableResponse loadTableResponse =
          icebergClient.updateTable(catalog, namespace, TestUtils.TABLE_NAME, request);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .contains("/metadata/00001-");
      // The new metadata version was written to the cloud location too.
      assertThat(
              Files.exists(localPathOf(loadTableResponse.tableMetadata().metadataFileLocation())))
          .isTrue();
      assertThat(loadTableResponse.tableMetadata().properties()).containsEntry("foo", "bar");
      assertThat(loadTableResponse.tableMetadata().schema().columns())
          .extracting(Types.NestedField::name)
          .containsExactly("id", "data", "category");

      TableInfo tableInfo = tableOperations.getTable(tableFullName);
      assertThat(tableInfo.getColumns())
          .extracting(ColumnInfo::getName)
          .containsExactly("id", "data", "category");
      assertThat(tableInfo.getProperties()).containsEntry("foo", "bar");

      // the new metadata location is what loadTable now returns
      LoadTableResponse reloaded =
          icebergClient.loadTable(catalog, namespace, TestUtils.TABLE_NAME);
      assertThat(reloaded.tableMetadata().metadataFileLocation())
          .isEqualTo(loadTableResponse.tableMetadata().metadataFileLocation());
      assertThat(reloaded.tableMetadata().properties()).containsEntry("foo", "bar");
    }

    // A commit whose requirements no longer hold fails with CommitFailedException
    {
      UpdateTableRequest request =
          new UpdateTableRequest(
              List.of(new UpdateRequirement.AssertTableUUID(UUID.randomUUID().toString())),
              List.of(new MetadataUpdate.SetProperties(Map.of("should", "fail"))));
      TestUtils.assertIcebergApiException(
          () -> icebergClient.updateTable(catalog, namespace, TestUtils.TABLE_NAME, request),
          409,
          "Requirement failed");
    }

    // Drop the table
    {
      icebergClient.dropTable(catalog, namespace, TestUtils.TABLE_NAME);
      assertThat(icebergClient.tableExists(catalog, namespace, TestUtils.TABLE_NAME)).isFalse();
      // The UC row is gone after the drop, so the load surfaces the repository's TABLE_NOT_FOUND
      // ("Table not found: ..."), not the service-level "does not exist" used when a row exists
      // without Iceberg metadata.
      TestUtils.assertIcebergApiException(
          () -> icebergClient.loadTable(catalog, namespace, TestUtils.TABLE_NAME),
          404,
          "not found");
    }

    // A create request without a location gets a server-assigned managed location under the catalog
    {
      CreateTableRequest request =
          CreateTableRequest.builder().withName("managed_iceberg_table").withSchema(schema).build();
      LoadTableResponse loadTableResponse = icebergClient.createTable(catalog, namespace, request);
      assertThat(loadTableResponse.tableMetadata().location())
          .startsWith("s3://" + CONFIGURED_BUCKET);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .contains("/metadata/00000-");
      assertThat(
              Files.exists(localPathOf(loadTableResponse.tableMetadata().metadataFileLocation())))
          .isTrue();

      TableInfo tableInfo =
          tableOperations.getTable(catalog + "." + namespace + ".managed_iceberg_table");
      assertThat(tableInfo.getDataSourceFormat()).isEqualTo(DataSourceFormat.ICEBERG);
      assertThat(tableInfo.getTableType()).isEqualTo(TableType.MANAGED);
      assertThat(tableInfo.getStorageLocation())
          .isEqualTo(loadTableResponse.tableMetadata().location());

      icebergClient.dropTable(catalog, namespace, "managed_iceberg_table");
    }
  }

  @Test
  public void testCreateOnGcsValidatesVendedToken() throws Exception {
    // A gs://-rooted catalog exercises the GCS branch of credential vending/validation: the fake
    // FileIO requires the vended GCS OAuth token before it will touch storage.
    String gcsRoot =
        "gs://" + CONFIGURED_BUCKET + testDirectoryRoot.toAbsolutePath() + "/gcscatalog";
    createCloudCatalog(
        "uc_iceberg_gcs", gcsRoot, testDirectoryRoot.toAbsolutePath().resolve("gcscatalog"));

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    LoadTableResponse created =
        icebergClient.createTable(
            "uc_iceberg_gcs",
            TestUtils.SCHEMA_NAME,
            CreateTableRequest.builder()
                .withName(TestUtils.TABLE_NAME)
                .withSchema(schema)
                .withLocation(gcsRoot + "/ext_iceberg")
                .build());
    String metadata = created.tableMetadata().metadataFileLocation();
    assertThat(metadata).startsWith("gs://" + CONFIGURED_BUCKET).contains("/metadata/00000-");
    assertThat(Files.exists(localPathOf(metadata))).isTrue();
    assertThat(created.config()).containsEntry(GCPProperties.GCS_OAUTH2_TOKEN, GCS_OAUTH_TOKEN);
  }

  @Test
  public void testStagedCreateAndCommit() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    // Stage the create (no location -> server-assigned managed location). No permanent table is
    // registered.
    TableMetadata staged;
    UUID stagingTableId;
    {
      CreateTableRequest request =
          CreateTableRequest.builder()
              .withName(TestUtils.TABLE_NAME)
              .withSchema(schema)
              .stageCreate()
              .build();
      LoadTableResponse loadTableResponse =
          icebergClient.createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request);
      staged = loadTableResponse.tableMetadata();
      assertThat(staged.metadataFileLocation()).isNull();
      assertThat(staged.location()).contains("/tables/");

      StagingTableDAO stagingTable = getStagingTableByLocation(staged.location());
      assertThat(stagingTable).isNotNull();
      assertThat(stagingTable.isStageCommitted()).isFalse();
      stagingTableId = stagingTable.getId();

      // the staged table is not yet a permanent UC table, so it is not loadable or listable
      TestUtils.assertIcebergApiException(
          () ->
              icebergClient.loadTable(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME),
          404);
    }

    // Commit the staged create: assert-create requirement + updates rebuilding the metadata
    {
      UpdateTableRequest request =
          new UpdateTableRequest(
              List.of(new UpdateRequirement.AssertTableDoesNotExist()),
              List.of(
                  new MetadataUpdate.AssignUUID(staged.uuid()),
                  new MetadataUpdate.UpgradeFormatVersion(staged.formatVersion()),
                  new MetadataUpdate.AddSchema(staged.schema()),
                  new MetadataUpdate.SetCurrentSchema(-1),
                  new MetadataUpdate.AddPartitionSpec(staged.spec()),
                  new MetadataUpdate.SetDefaultPartitionSpec(-1),
                  new MetadataUpdate.AddSortOrder(staged.sortOrder()),
                  new MetadataUpdate.SetDefaultSortOrder(-1),
                  new MetadataUpdate.SetLocation(staged.location()),
                  new MetadataUpdate.SetProperties(Map.of("staged", "true"))));
      LoadTableResponse loadTableResponse =
          icebergClient.updateTable(
              TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, request);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .contains("/metadata/00000-");
      assertThat(loadTableResponse.tableMetadata().uuid()).isEqualTo(staged.uuid());
      assertThat(loadTableResponse.tableMetadata().properties()).containsEntry("staged", "true");

      // the table is now registered in UC as a managed Iceberg table and loadable
      TableInfo tableInfo = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
      assertThat(tableInfo.getDataSourceFormat()).isEqualTo(DataSourceFormat.ICEBERG);
      assertThat(tableInfo.getTableType()).isEqualTo(TableType.MANAGED);
      assertThat(tableInfo.getTableId()).isEqualTo(stagingTableId.toString());
      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        assertThat(session.get(StagingTableDAO.class, stagingTableId).isStageCommitted()).isTrue();
      }
      assertThat(
              icebergClient.tableExists(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME))
          .isTrue();

      // replaying the create commit loses the race: 409 CommitFailedException
      TestUtils.assertIcebergApiException(
          () ->
              icebergClient.updateTable(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, request),
          409,
          "Requirement failed");
    }

    // staging a create for an existing table is a conflict
    {
      CreateTableRequest request =
          CreateTableRequest.builder()
              .withName(TestUtils.TABLE_NAME)
              .withSchema(schema)
              .stageCreate()
              .build();
      TestUtils.assertIcebergApiException(
          () -> icebergClient.createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request),
          409,
          "already exists");
    }
  }

  @Test
  public void testConcurrentCommitsSerializeWithCompareAndSwap()
      throws ApiException, IOException, InterruptedException, ExecutionException, TimeoutException {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    String location = Files.createTempDirectory("iceberg-rest-concurrent").toUri().toString();

    // Create the table; its current-schema-id is 0.
    CreateTableRequest createRequest =
        CreateTableRequest.builder()
            .withName(TestUtils.TABLE_NAME)
            .withSchema(schema)
            .withLocation(location)
            .build();
    LoadTableResponse created =
        icebergClient.createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, createRequest);
    assertThat(created.tableMetadata().metadataFileLocation()).contains("/metadata/00000-");

    // Fire N commits at once. Each asserts the original current-schema-id (0) and bumps the schema
    // to a new id, so the commits are mutually exclusive: only the first to land can both satisfy
    // its requirement and win the metadata-location compare-and-swap in
    // TableRepository#commitIcebergTable. A loser fails either because it raced and lost
    // the CAS, or because it read post-winner state where assert-current-schema-id no longer holds;
    // both surface as 409 CommitFailedException. The CyclicBarrier releases the threads together to
    // exercise the CAS path when timing allows, but the outcome is deterministic either way.
    int concurrency = 8;
    Schema bumpedSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "added", Types.StringType.get()));
    CyclicBarrier barrier = new CyclicBarrier(concurrency);
    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
    List<Future<ErrorResponse>> futures = new ArrayList<>();
    for (int i = 0; i < concurrency; i++) {
      futures.add(
          pool.submit(
              () -> {
                UpdateTableRequest request =
                    new UpdateTableRequest(
                        List.of(new UpdateRequirement.AssertCurrentSchemaID(0)),
                        List.of(
                            new MetadataUpdate.AddSchema(bumpedSchema),
                            new MetadataUpdate.SetCurrentSchema(-1)));
                barrier.await();
                try {
                  icebergClient.updateTable(
                      TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, request);
                  return null;
                } catch (ApiException e) {
                  return ErrorResponseParser.fromJson(e.getResponseBody());
                }
              }));
    }

    int successes = 0;
    int conflicts = 0;
    for (Future<ErrorResponse> future : futures) {
      ErrorResponse error = future.get(30, TimeUnit.SECONDS);
      if (error == null) {
        successes++;
      } else {
        // A commit either wins (null error) or loses cleanly as a 409 CommitFailedException; any
        // other status or error type would mean the contention surfaced as a server error.
        assertThat(error.code()).isEqualTo(409);
        assertThat(error.type()).isEqualTo("CommitFailedException");
        conflicts++;
      }
    }
    pool.shutdown();
    assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

    // Exactly one commit wins and the rest lose cleanly: no lost updates, no double-applies.
    assertThat(successes).isEqualTo(1);
    assertThat(conflicts).isEqualTo(concurrency - 1);

    // The single winner advanced the table to version 1 with the bumped schema; losers left no
    // trace (their metadata files were rolled back), so the table is loadable and consistent.
    LoadTableResponse loaded =
        icebergClient.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(loaded.tableMetadata().metadataFileLocation()).contains("/metadata/00001-");
    assertThat(loaded.tableMetadata().schema().columns()).hasSize(2);
  }

  @Test
  public void testReportMetrics() throws Exception {
    setUniformMetadata(createTable(TestUtils.TABLE_NAME), writeIcebergMetadata());

    // Per the REST spec, a report is acknowledged with 204 No Content.
    icebergClient.reportMetrics(
        TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, scanReport());
    icebergClient.reportMetrics(
        TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, commitReport());

    // A body that isn't a metrics report is rejected rather than silently accepted.
    HttpResponse<String> badBody =
        TestUtils.sendRaw(
            serverConfig,
            "POST",
            ICEBERG_PREFIX
                + "/v1/catalogs/"
                + TestUtils.CATALOG_NAME
                + "/namespaces/"
                + TestUtils.SCHEMA_NAME
                + "/tables/"
                + TestUtils.TABLE_NAME
                + "/metrics",
            Optional.of("{\"foo\":\"bar\"}"));
    assertThat(badBody.statusCode()).isEqualTo(400);

    // A table UC knows about but doesn't serve as an Iceberg table is a 404, like loadTable.
    createTable("plainTable");
    TestUtils.assertIcebergApiException(
        () ->
            icebergClient.reportMetrics(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "plainTable", scanReport()),
        404,
        "does not exist");

    // A table that doesn't exist at all is a 404 too.
    TestUtils.assertIcebergApiException(
        () ->
            icebergClient.reportMetrics(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "missingTable", scanReport()),
        404);
  }

  @Test
  public void testListNamespacesReturnsEveryNamespace() throws ApiException, IOException {
    // One namespace more than the repository returns in a single page (on top of the default
    // SCHEMA_NAME the base class created).
    List<String> expected = new ArrayList<>();
    for (int i = 0; i <= PAGE_SIZE; i++) {
      String name = "schema_%03d".formatted(i);
      icebergClient.createNamespace(TestUtils.CATALOG_NAME, name);
      expected.add(name);
    }
    // The base class's default SCHEMA_NAME ("uc_testschema") sorts after every "schema_NNN", so it
    // comes last in the sorted listing.
    expected.add(TestUtils.SCHEMA_NAME);

    ListNamespacesResponse listed = icebergClient.listNamespaces(TestUtils.CATALOG_NAME);
    // The listing pages to the end and returns exactly the expected namespaces, in order -- so a
    // paging regression that dropped, duplicated, or reordered entries beyond the first page fails.
    assertThat(listed.namespaces()).map(Namespace::toString).containsExactlyElementsOf(expected);
  }

  @Test
  public void testListTablesReturnsTablesBeyondTheFirstPage()
      throws ApiException, IOException, URISyntaxException {
    // Fill the first page with tables the Iceberg endpoints don't serve, so that the only uniform
    // table sorts onto the second page
    for (int i = 0; i < PAGE_SIZE; i++) {
      createTable("delta_%03d".formatted(i));
    }
    setUniformMetadata(createTable("uniform_table"), writeIcebergMetadata());

    ListTablesResponse listed =
        icebergClient.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
    assertThat(listed.identifiers())
        .containsExactly(TableIdentifier.of(Namespace.of(TestUtils.SCHEMA_NAME), "uniform_table"));
  }

  /** Maps a cloud location back to the local path the fake FileIO wrote it to. */
  private Path localPathOf(String cloudLocation) {
    return mappingFileOperations.localPathOf(NormalizedURL.from(cloudLocation));
  }

  private StagingTableDAO getStagingTableByLocation(String location) {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      return session
          .createQuery(
              "FROM StagingTableDAO WHERE stagingLocation = :location", StagingTableDAO.class)
          .setParameter("location", location)
          .uniqueResult();
    }
  }

  private TableInfoDAO getTableByName(Session session, String name) {
    return session
        .createQuery("FROM TableInfoDAO WHERE name = :name", TableInfoDAO.class)
        .setParameter("name", name)
        .getSingleResult();
  }

  private static ReportMetricsRequest scanReport() {
    return ReportMetricsRequest.of(
        ImmutableScanReport.builder()
            .tableName(TestUtils.TABLE_NAME)
            .schemaId(0)
            .addProjectedFieldIds(1)
            .addProjectedFieldNames("as_int")
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build());
  }

  private static ReportMetricsRequest commitReport() {
    return ReportMetricsRequest.of(
        ImmutableCommitReport.builder()
            .tableName(TestUtils.TABLE_NAME)
            .snapshotId(23L)
            .sequenceNumber(4L)
            .operation("append")
            .commitMetrics(CommitMetricsResult.from(CommitMetrics.noop(), Map.of()))
            .build());
  }

  /** Makes a table visible to the Iceberg endpoints by giving it uniform Iceberg metadata. */
  private void setUniformMetadata(TableInfo tableInfo, Path metadataFile) {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      Transaction tx = session.beginTransaction();
      UUID tableId = UUID.fromString(Objects.requireNonNull(tableInfo.getTableId()));
      TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableId);
      assertThat(tableInfoDAO).isNotNull();
      tableInfoDAO.setUniformIcebergMetadataLocation(metadataFile.toUri().toString());
      session.merge(tableInfoDAO);
      tx.commit();
    }
  }

  /** Creates a plain UC table, i.e. one without uniform Iceberg metadata. */
  private TableInfo createTable(String tableName) throws ApiException, IOException {
    return tableOperations.createTable(
        new CreateTable()
            .name(tableName)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("as_int")
                        .typeText("INTEGER")
                        .typeJson(
                            "{\"name\":\"as_int\",\"type\":\"integer\","
                                + "\"nullable\":true,\"metadata\":{}}")
                        .typeName(ColumnTypeName.INT)
                        .typePrecision(10)
                        .typeScale(0)
                        .position(0)
                        .nullable(true)))
            .storageLocation(icebergTableLocation.toString())
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA));
  }

  private Path writeIcebergMetadata() throws IOException, URISyntaxException {
    Path source =
        Path.of(
            Objects.requireNonNull(this.getClass().getResource("/iceberg.metadata.json")).toURI());
    Path metadataFile = icebergTableLocation.resolve("iceberg.metadata.json");
    String tableLocation = NormalizedURL.from(icebergTableLocation.toUri()).toString();
    String metadata =
        Files.readString(source).replace("file:/tmp/uniform_iceberg_table", tableLocation);
    return Files.writeString(metadataFile, metadata);
  }
}
