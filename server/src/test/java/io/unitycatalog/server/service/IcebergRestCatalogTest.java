package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.auth.AuthToken;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.service.iceberg.IcebergObjectMapper;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
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
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.CommitMetrics;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequestParser;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
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

public class IcebergRestCatalogTest extends BaseServerTest {

  private static final String TEST_BASE_PREFIX = "/v1/catalogs/" + TestUtils.CATALOG_NAME;
  private static final String TEST_BASE_NON_PREFIX = "/v1";
  private static final int PAGE_SIZE = PagedListingHelper.DEFAULT_PAGE_SIZE;

  @TempDir private Path icebergTableLocation;

  protected CatalogOperations catalogOperations;
  protected SchemaOperations schemaOperations;
  protected TableOperations tableOperations;
  private WebClient client;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // Native Iceberg REST writes are opt-in in production; this integration suite exercises them.
    serverProperties.setProperty(Property.ICEBERG_TABLE_ENABLED.getKey(), "true");
  }

  @BeforeEach
  public void setUp() {
    super.setUp();
    String uri = serverConfig.getServerUrl() + "/api/2.1/unity-catalog/iceberg";
    String token = serverConfig.getAuthToken();
    catalogOperations = new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
    schemaOperations = new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
    tableOperations = new SdkTableOperations(TestUtils.createApiClient(serverConfig));
    client = WebClient.builder(uri).auth(AuthToken.ofOAuth2(token)).build();
    cleanUp();
  }

  protected void cleanUp() {
    try {
      if (catalogOperations.getCatalog(TestUtils.CATALOG_NAME) != null) {
        catalogOperations.deleteCatalog(TestUtils.CATALOG_NAME, Optional.of(true));
      }
    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  public void testConfig() {
    // successful test of getting client config with prefix when passing in warehouse param
    AggregatedHttpResponse resp =
        client.get("/v1/config?warehouse=" + TestUtils.CATALOG_NAME).aggregate().join();
    assertThat(resp.contentUtf8())
        .isEqualTo(
            "{\"defaults\":{},\"overrides\":{\"prefix\":\"catalogs/"
                + TestUtils.CATALOG_NAME
                + "\"}"
                + ",\"endpoints\":["
                + "\"GET /v1/{prefix}/namespaces\","
                + "\"GET /v1/{prefix}/namespaces/{namespace}\""
                + ",\"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}\","
                + "\"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}\","
                + "\"GET /v1/{prefix}/namespaces/{namespace}/views/{view}\","
                + "\"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics\","
                + "\"GET /v1/{prefix}/namespaces/{namespace}/tables\","
                + "\"POST /v1/{prefix}/namespaces\","
                + "\"POST /v1/{prefix}/namespaces/{namespace}/tables\","
                + "\"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}\","
                + "\"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}\""
                + "]}");

    // not setting warehouse param should result in 400 BadRequestException
    resp = client.get("/v1/config").aggregate().join();
    assertThat(resp.status().code()).isEqualTo(400);
    ErrorResponse errorResponse = ErrorResponseParser.fromJson(resp.contentUtf8());
    assertThat(errorResponse.type()).isEqualTo(BadRequestException.class.getSimpleName());
  }

  @Test
  public void testNamespaces() throws ApiException, IOException {
    CreateCatalog createCatalog =
        new CreateCatalog()
            .name(TestUtils.CATALOG_NAME)
            .comment(TestUtils.COMMENT)
            .properties(TestUtils.PROPERTIES);
    CatalogInfo catalogInfo = catalogOperations.createCatalog(createCatalog);
    assertThat(catalogInfo.getName()).isEqualTo(createCatalog.getName());
    assertThat(catalogInfo.getComment()).isEqualTo(createCatalog.getComment());
    assertThat(catalogInfo.getProperties()).isEqualTo(createCatalog.getProperties());

    CreateSchema createSchema =
        new CreateSchema()
            .catalogName(TestUtils.CATALOG_NAME)
            .name(TestUtils.SCHEMA_NAME)
            .properties(TestUtils.PROPERTIES);
    SchemaInfo schemaInfo = schemaOperations.createSchema(createSchema);
    assertThat(schemaInfo.getName()).isEqualTo(createSchema.getName());
    assertThat(schemaInfo.getCatalogName()).isEqualTo(createSchema.getCatalogName());
    assertThat(schemaInfo.getFullName()).isEqualTo(TestUtils.SCHEMA_FULL_NAME);
    assertThat(schemaInfo.getProperties()).isEqualTo(createSchema.getProperties());
    // GetNamespace
    {
      AggregatedHttpResponse resp =
          client.get(TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(200);
      assertThat(
              IcebergObjectMapper.mapper()
                  .readValue(resp.contentUtf8(), GetNamespaceResponse.class))
          .asString()
          .isEqualTo(
              GetNamespaceResponse.builder()
                  .withNamespace(Namespace.of(TestUtils.SCHEMA_NAME))
                  .setProperties(TestUtils.PROPERTIES)
                  .build()
                  .toString());

      // non-prefixed URL should result in 404
      resp =
          client
              .get(TEST_BASE_NON_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME)
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(404);
    }

    // ListNamespaces
    {
      AggregatedHttpResponse resp = client.get(TEST_BASE_PREFIX + "/namespaces").aggregate().join();
      assertThat(resp.status().code()).isEqualTo(200);
      assertThat(
              IcebergObjectMapper.mapper()
                  .readValue(resp.contentUtf8(), ListNamespacesResponse.class))
          .asString()
          .isEqualTo(
              ListNamespacesResponse.builder()
                  .add(Namespace.of(TestUtils.SCHEMA_NAME))
                  .build()
                  .toString());

      // non-prefixed URL should result in 404
      resp = client.get(TEST_BASE_NON_PREFIX + "/namespaces").aggregate().join();
      assertThat(resp.status().code()).isEqualTo(404);
    }
  }

  @Test
  public void testTable() throws ApiException, IOException {
    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    catalogOperations.createCatalog(createCatalog);
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));
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
    {
      AggregatedHttpResponse resp =
          client
              .head(
                  TEST_BASE_PREFIX
                      + "/namespaces/"
                      + TestUtils.SCHEMA_NAME
                      + "/tables/"
                      + TestUtils.TABLE_NAME)
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(404);
    }
    {
      AggregatedHttpResponse resp =
          client
              .get(
                  TEST_BASE_PREFIX
                      + "/namespaces/"
                      + TestUtils.SCHEMA_NAME
                      + "/tables/"
                      + TestUtils.TABLE_NAME)
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(404);
      ErrorResponse errorResponse = ErrorResponseParser.fromJson(resp.contentUtf8());
      assertThat(errorResponse.type()).isEqualTo(NoSuchTableException.class.getSimpleName());
    }

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
    {
      AggregatedHttpResponse resp =
          client
              .head(
                  TEST_BASE_PREFIX
                      + "/namespaces/"
                      + TestUtils.SCHEMA_NAME
                      + "/tables/"
                      + TestUtils.TABLE_NAME)
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(200);
    }
    // metadata is valid metadata content and metadata location matches
    {
      AggregatedHttpResponse resp =
          client
              .get(
                  TEST_BASE_PREFIX
                      + "/namespaces/"
                      + TestUtils.SCHEMA_NAME
                      + "/tables/"
                      + TestUtils.TABLE_NAME)
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse loadTableResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .isEqualTo(metadataFile.toString());

      // non-prefixed URL should result in 404
      resp =
          client
              .get(
                  TEST_BASE_NON_PREFIX
                      + "/namespaces/"
                      + TestUtils.SCHEMA_NAME
                      + "/tables/"
                      + TestUtils.TABLE_NAME)
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(404);
    }

    // List uniform tables
    {
      AggregatedHttpResponse resp =
          client
              .get(TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME + "/tables")
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(200);
      ListTablesResponse loadTableResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), ListTablesResponse.class);
      assertThat(loadTableResponse.identifiers())
          .containsExactly(TableIdentifier.of(TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME));

      // non-prefixed URL should result in 404
      resp =
          client
              .get(TEST_BASE_NON_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME + "/tables")
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(404);
    }

    // UniForm-derived Iceberg metadata is read-only: commits and drops through the Iceberg REST
    // catalog must be rejected.
    {
      String tablePath =
          TEST_BASE_PREFIX
              + "/namespaces/"
              + TestUtils.SCHEMA_NAME
              + "/tables/"
              + TestUtils.TABLE_NAME;
      UpdateTableRequest commitRequest =
          new UpdateTableRequest(
              List.of(), List.of(new MetadataUpdate.SetProperties(Map.of("foo", "bar"))));
      AggregatedHttpResponse resp =
          postJson(tablePath, IcebergObjectMapper.mapper().writeValueAsString(commitRequest));
      assertThat(resp.status().code()).isEqualTo(400);

      resp = client.delete(tablePath).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(400);
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
    AggregatedHttpResponse conflictResp =
        client
            .get(
                TEST_BASE_PREFIX
                    + "/namespaces/"
                    + TestUtils.SCHEMA_NAME
                    + "/tables/"
                    + TestUtils.TABLE_NAME)
            .aggregate()
            .join();
    assertThat(conflictResp.status().code()).isEqualTo(400);
    assertThat(ErrorResponseParser.fromJson(conflictResp.contentUtf8()).message())
        .contains("persisted table location");
  }

  @Test
  public void testIcebergTableWriteLifecycle() throws ApiException, IOException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));

    String namespacesPath = TEST_BASE_PREFIX + "/namespaces";
    String tablesPath = namespacesPath + "/" + TestUtils.SCHEMA_NAME + "/tables";
    String tablePath = tablesPath + "/" + TestUtils.TABLE_NAME;

    // Create the namespace through the Iceberg REST catalog
    {
      CreateNamespaceRequest request =
          CreateNamespaceRequest.builder()
              .withNamespace(Namespace.of(TestUtils.SCHEMA_NAME))
              .setProperties(TestUtils.PROPERTIES)
              .build();
      AggregatedHttpResponse resp =
          postJson(namespacesPath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).isEqualTo(200);
      CreateNamespaceResponse createNamespaceResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), CreateNamespaceResponse.class);
      assertThat(createNamespaceResponse.namespace())
          .isEqualTo(Namespace.of(TestUtils.SCHEMA_NAME));

      // creating it again is a conflict
      resp = postJson(namespacesPath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).isEqualTo(409);
      assertThat(ErrorResponseParser.fromJson(resp.contentUtf8()).type())
          .isEqualTo(AlreadyExistsException.class.getSimpleName());
    }

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    String location = Files.createTempDirectory("iceberg-rest-table").toUri().toString();

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
      AggregatedHttpResponse resp =
          postJson(tablesPath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).as(resp.contentUtf8()).isEqualTo(200);
      LoadTableResponse loadTableResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation()).isNull();
      assertThat(client.get(tablePath).aggregate().join().status().code()).isEqualTo(404);
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
      AggregatedHttpResponse resp =
          postJson(tablesPath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).as(resp.contentUtf8()).isEqualTo(200);
      LoadTableResponse loadTableResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      initialMetadataLocation = loadTableResponse.tableMetadata().metadataFileLocation();
      assertThat(initialMetadataLocation).contains("/metadata/00000-");
      assertThat(loadTableResponse.tableMetadata().schema().columns()).hasSize(2);
      assertThat(loadTableResponse.tableMetadata().properties())
          .containsEntry("created-by", "iceberg-rest-test");
      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        TableInfoDAO tableInfoDAO = getTableByName(session, TestUtils.TABLE_NAME);
        assertThat(tableInfoDAO.getUniformIcebergMetadataLocation())
            .isEqualTo(NormalizedURL.from(initialMetadataLocation).toString());
      }

      // creating it again is a conflict
      resp = postJson(tablesPath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).isEqualTo(409);
      assertThat(ErrorResponseParser.fromJson(resp.contentUtf8()).type())
          .isEqualTo(AlreadyExistsException.class.getSimpleName());
    }

    // The table is registered in UC as a native Iceberg table with converted columns
    {
      TableInfo tableInfo = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
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
      AggregatedHttpResponse resp = client.head(tablePath).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(200);

      resp = client.get(tablePath).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse loadTableResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .isEqualTo(initialMetadataLocation);
      tableUuid = loadTableResponse.tableMetadata().uuid();

      resp = client.get(tablesPath).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(200);
      ListTablesResponse listTablesResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), ListTablesResponse.class);
      assertThat(listTablesResponse.identifiers())
          .containsExactly(TableIdentifier.of(TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME));
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
      AggregatedHttpResponse resp =
          postJson(tablePath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse loadTableResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .contains("/metadata/00001-");
      assertThat(loadTableResponse.tableMetadata().properties()).containsEntry("foo", "bar");
      assertThat(loadTableResponse.tableMetadata().schema().columns())
          .extracting(Types.NestedField::name)
          .containsExactly("id", "data", "category");

      TableInfo tableInfo = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
      assertThat(tableInfo.getColumns())
          .extracting(ColumnInfo::getName)
          .containsExactly("id", "data", "category");
      assertThat(tableInfo.getProperties()).containsEntry("foo", "bar");

      // the new metadata location is what loadTable now returns
      resp = client.get(tablePath).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse reloaded =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
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
      AggregatedHttpResponse resp =
          postJson(tablePath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).isEqualTo(409);
      assertThat(ErrorResponseParser.fromJson(resp.contentUtf8()).type())
          .isEqualTo(CommitFailedException.class.getSimpleName());
    }

    // Drop the table
    {
      AggregatedHttpResponse resp = client.delete(tablePath).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(204);

      resp = client.head(tablePath).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(404);

      resp = client.get(tablePath).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(404);
      assertThat(ErrorResponseParser.fromJson(resp.contentUtf8()).type())
          .isEqualTo(NoSuchTableException.class.getSimpleName());
    }

    // A create request without a location gets a server-assigned managed location
    {
      String managedTablePath = tablesPath + "/managed_iceberg_table";
      CreateTableRequest request =
          CreateTableRequest.builder().withName("managed_iceberg_table").withSchema(schema).build();
      AggregatedHttpResponse resp =
          postJson(tablesPath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse loadTableResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      assertThat(loadTableResponse.tableMetadata().location()).contains("/tables/");
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .contains("/metadata/00000-");

      TableInfo tableInfo =
          tableOperations.getTable(
              TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".managed_iceberg_table");
      assertThat(tableInfo.getDataSourceFormat()).isEqualTo(DataSourceFormat.ICEBERG);
      assertThat(tableInfo.getTableType()).isEqualTo(TableType.MANAGED);
      assertThat(tableInfo.getStorageLocation())
          .isEqualTo(loadTableResponse.tableMetadata().location());

      resp = client.delete(managedTablePath).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(204);
    }
  }

  @Test
  public void testStagedCreateAndCommit() throws ApiException, IOException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));

    String tablesPath = TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME + "/tables";
    String tablePath = tablesPath + "/" + TestUtils.TABLE_NAME;
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
      AggregatedHttpResponse resp =
          postJson(tablesPath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse loadTableResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      staged = loadTableResponse.tableMetadata();
      assertThat(staged.metadataFileLocation()).isNull();
      assertThat(staged.location()).contains("/tables/");

      StagingTableDAO stagingTable = getStagingTableByLocation(staged.location());
      assertThat(stagingTable).isNotNull();
      assertThat(stagingTable.isStageCommitted()).isFalse();
      stagingTableId = stagingTable.getId();

      // the staged table is not yet a permanent UC table, so it is not loadable or listable
      assertThat(client.get(tablePath).aggregate().join().status().code()).isEqualTo(404);
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
      AggregatedHttpResponse resp =
          postJson(tablePath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse loadTableResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
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
      assertThat(client.get(tablePath).aggregate().join().status().code()).isEqualTo(200);

      // replaying the create commit loses the race: 409 CommitFailedException
      resp = postJson(tablePath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).isEqualTo(409);
      assertThat(ErrorResponseParser.fromJson(resp.contentUtf8()).type())
          .isEqualTo(CommitFailedException.class.getSimpleName());
    }

    // staging a create for an existing table is a conflict
    {
      CreateTableRequest request =
          CreateTableRequest.builder()
              .withName(TestUtils.TABLE_NAME)
              .withSchema(schema)
              .stageCreate()
              .build();
      AggregatedHttpResponse resp =
          postJson(tablesPath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).isEqualTo(409);
      assertThat(ErrorResponseParser.fromJson(resp.contentUtf8()).type())
          .isEqualTo(AlreadyExistsException.class.getSimpleName());
    }
  }

  @Test
  public void testConcurrentCommitsSerializeWithCompareAndSwap()
      throws ApiException, IOException, InterruptedException, ExecutionException, TimeoutException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));

    String tablesPath = TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME + "/tables";
    String tablePath = tablesPath + "/" + TestUtils.TABLE_NAME;
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    String location = Files.createTempDirectory("iceberg-rest-concurrent").toUri().toString();

    // Create the table; its current-schema-id is 0.
    CreateTableRequest createRequest =
        CreateTableRequest.builder()
            .withName(TestUtils.TABLE_NAME)
            .withSchema(schema)
            .withLocation(location)
            .build();
    AggregatedHttpResponse createResp =
        postJson(tablesPath, IcebergObjectMapper.mapper().writeValueAsString(createRequest));
    assertThat(createResp.status().code()).isEqualTo(200);
    LoadTableResponse created =
        IcebergObjectMapper.mapper().readValue(createResp.contentUtf8(), LoadTableResponse.class);
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
    List<Future<Integer>> futures = new ArrayList<>();
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
                String body = IcebergObjectMapper.mapper().writeValueAsString(request);
                barrier.await();
                return postJson(tablePath, body).status().code();
              }));
    }

    int successes = 0;
    int conflicts = 0;
    for (Future<Integer> future : futures) {
      int code = future.get(30, TimeUnit.SECONDS);
      // A commit either wins (200) or loses cleanly (409 CommitFailedException); any other status
      // would mean the contention surfaced as a server error.
      assertThat(code).isIn(200, 409);
      if (code == 200) {
        successes++;
      } else {
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
    AggregatedHttpResponse loadResp = client.get(tablePath).aggregate().join();
    assertThat(loadResp.status().code()).isEqualTo(200);
    LoadTableResponse loaded =
        IcebergObjectMapper.mapper().readValue(loadResp.contentUtf8(), LoadTableResponse.class);
    assertThat(loaded.tableMetadata().metadataFileLocation()).contains("/metadata/00001-");
    assertThat(loaded.tableMetadata().schema().columns()).hasSize(2);
  }

  @Test
  public void testReportMetrics() throws Exception {
    createUniformIcebergTable();
    String metricsPath =
        TEST_BASE_PREFIX
            + "/namespaces/"
            + TestUtils.SCHEMA_NAME
            + "/tables/"
            + TestUtils.TABLE_NAME
            + "/metrics";

    // Per the REST spec, a report is acknowledged with 204 No Content.
    assertThat(postJson(metricsPath, scanReportJson()).status().code()).isEqualTo(204);
    assertThat(postJson(metricsPath, commitReportJson()).status().code()).isEqualTo(204);

    // A body that isn't a metrics report is rejected rather than silently accepted.
    assertThat(postJson(metricsPath, "{\"foo\":\"bar\"}").status().code()).isEqualTo(400);

    // A table UC knows about but doesn't serve as an Iceberg table is a 404, like loadTable.
    createTable("plainTable");
    AggregatedHttpResponse resp =
        postJson(
            TEST_BASE_PREFIX
                + "/namespaces/"
                + TestUtils.SCHEMA_NAME
                + "/tables/plainTable/metrics",
            scanReportJson());
    assertThat(resp.status().code()).isEqualTo(404);
    assertThat(ErrorResponseParser.fromJson(resp.contentUtf8()).type())
        .isEqualTo(NoSuchTableException.class.getSimpleName());

    // A table that doesn't exist at all is a 404 too.
    resp =
        postJson(
            TEST_BASE_PREFIX
                + "/namespaces/"
                + TestUtils.SCHEMA_NAME
                + "/tables/missingTable/metrics",
            scanReportJson());
    assertThat(resp.status().code()).isEqualTo(404);

    // The non-prefixed URL isn't routed, matching the other Iceberg endpoints.
    resp =
        postJson(
            TEST_BASE_NON_PREFIX
                + "/namespaces/"
                + TestUtils.SCHEMA_NAME
                + "/tables/"
                + TestUtils.TABLE_NAME
                + "/metrics",
            scanReportJson());
    assertThat(resp.status().code()).isEqualTo(404);
  }

  @Test
  public void testListNamespacesReturnsEveryNamespace() throws ApiException, IOException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    // One namespace more than the repository returns in a single page
    List<String> created = new ArrayList<>();
    for (int i = 0; i <= PAGE_SIZE; i++) {
      String name = "schema_%03d".formatted(i);
      schemaOperations.createSchema(
          new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(name));
      created.add(name);
    }

    AggregatedHttpResponse resp = client.get(TEST_BASE_PREFIX + "/namespaces").aggregate().join();

    assertThat(resp.status().code()).isEqualTo(200);
    ListNamespacesResponse listed =
        IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), ListNamespacesResponse.class);
    assertThat(listed.namespaces()).map(Namespace::toString).containsExactlyElementsOf(created);
  }

  @Test
  public void testListTablesReturnsTablesBeyondTheFirstPage()
      throws ApiException, IOException, URISyntaxException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));

    // Fill the first page with tables the Iceberg endpoints don't serve, so that the only uniform
    // table sorts onto the second page
    for (int i = 0; i < PAGE_SIZE; i++) {
      createTable("delta_%03d".formatted(i));
    }
    setUniformMetadata(createTable("uniform_table"), writeIcebergMetadata());

    AggregatedHttpResponse resp =
        client
            .get(TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME + "/tables")
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
    ListTablesResponse listed =
        IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), ListTablesResponse.class);
    assertThat(listed.identifiers())
        .containsExactly(TableIdentifier.of(Namespace.of(TestUtils.SCHEMA_NAME), "uniform_table"));
  }

  private AggregatedHttpResponse postJson(String path, String body) {
    return client
        .execute(
            RequestHeaders.builder(HttpMethod.POST, path).contentType(MediaType.JSON).build(), body)
        .aggregate()
        .join();
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

  private static String scanReportJson() {
    return ReportMetricsRequestParser.toJson(
        ReportMetricsRequest.of(
            ImmutableScanReport.builder()
                .tableName(TestUtils.TABLE_NAME)
                .schemaId(0)
                .addProjectedFieldIds(1)
                .addProjectedFieldNames("as_int")
                .snapshotId(23L)
                .filter(Expressions.alwaysTrue())
                .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
                .build()));
  }

  private static String commitReportJson() {
    return ReportMetricsRequestParser.toJson(
        ReportMetricsRequest.of(
            ImmutableCommitReport.builder()
                .tableName(TestUtils.TABLE_NAME)
                .snapshotId(23L)
                .sequenceNumber(4L)
                .operation("append")
                .commitMetrics(CommitMetricsResult.from(CommitMetrics.noop(), Map.of()))
                .build()));
  }

  /** Creates a table that the Iceberg endpoints see, i.e. one with uniform Iceberg metadata. */
  private void createUniformIcebergTable() throws IOException, URISyntaxException, ApiException {
    Path metadataFile = writeIcebergMetadata();
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));
    setUniformMetadata(createTable(TestUtils.TABLE_NAME), metadataFile);
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
