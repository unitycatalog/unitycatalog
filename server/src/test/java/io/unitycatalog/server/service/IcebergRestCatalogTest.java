package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpHeaderNames;
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
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URI;
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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.CommitMetrics;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
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
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.types.Types;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

public class IcebergRestCatalogTest extends BaseServerTest {

  private static final String ICEBERG_BASE_PATH = "/api/2.1/unity-catalog/iceberg";
  private static final String TEST_BASE_PREFIX = "/v1/catalogs/" + TestUtils.CATALOG_NAME;
  private static final String TEST_BASE_NON_PREFIX = "/v1";
  private static final int PAGE_SIZE = PagedListingHelper.DEFAULT_PAGE_SIZE;

  @TempDir private Path icebergTableLocation;

  protected CatalogOperations catalogOperations;
  protected SchemaOperations schemaOperations;
  protected TableOperations tableOperations;
  private WebClient client;
  // Typed client for the standard REST calls, so CRUD tests read as catalog operations rather than
  // hand-built HTTP. Tests that assert HTTP-level behavior (status/headers, invalid input, unrouted
  // paths) keep using the raw {@code client} below.
  private IcebergRestClient icebergClient;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // Native Iceberg REST writes are opt-in in production; this integration suite exercises them.
    serverProperties.setProperty(Property.ICEBERG_TABLE_ENABLED.getKey(), "true");
  }

  @BeforeEach
  public void setUp() {
    super.setUp();
    String uri = serverConfig.getServerUrl() + ICEBERG_BASE_PATH;
    String token = serverConfig.getAuthToken();
    catalogOperations = new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
    schemaOperations = new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
    tableOperations = new SdkTableOperations(TestUtils.createApiClient(serverConfig));
    client = WebClient.builder(uri).auth(AuthToken.ofOAuth2(token)).build();
    icebergClient = new IcebergRestClient(serverConfig);
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
  public void testConfig() throws ApiException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));

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
                + "\"GET /v1/{prefix}/namespaces/{namespace}\","
                + "\"HEAD /v1/{prefix}/namespaces/{namespace}\""
                + ",\"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}\","
                + "\"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}\","
                + "\"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials\","
                + "\"GET /v1/{prefix}/namespaces/{namespace}/views/{view}\","
                + "\"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics\","
                + "\"GET /v1/{prefix}/namespaces/{namespace}/tables\","
                + "\"POST /v1/{prefix}/namespaces\","
                + "\"DELETE /v1/{prefix}/namespaces/{namespace}\","
                + "\"POST /v1/{prefix}/namespaces/{namespace}/properties\","
                + "\"POST /v1/{prefix}/namespaces/{namespace}/tables\","
                + "\"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}\","
                + "\"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}\","
                + "\"POST /v1/{prefix}/tables/rename\""
                + "]}");

    // not setting warehouse param should result in 400 BadRequestException
    resp = client.get("/v1/config").aggregate().join();
    assertErrorType(resp, 400, BadRequestException.class);
    ErrorResponse errorResponse = ErrorResponseParser.fromJson(resp.contentUtf8());
    assertThat(errorResponse.type()).isEqualTo(BadRequestException.class.getSimpleName());

    // A warehouse that does not exist is a 404, which is what the client reads as no such
    // warehouse. Answering 200 would hand back a prefix whose every request fails instead.
    resp = client.get("/v1/config?warehouse=noSuchCatalog").aggregate().join();
    assertThat(resp.status().code()).isEqualTo(404);
    assertThat(ErrorResponseParser.fromJson(resp.contentUtf8()).message())
        .contains("noSuchCatalog");
  }

  @Test
  public void testNamespaceExists() throws ApiException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));

    // The REST spec answers this HEAD with 204 and no content. Served by the GET route it answered
    // 200 and described the body of the namespace response, which a HEAD must not carry.
    AggregatedHttpResponse resp =
        client.head(TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME).aggregate().join();
    assertThat(resp.status().code()).isEqualTo(204);
    assertThat(resp.contentUtf8()).isEmpty();
    assertThat(resp.headers().contentLength()).isLessThanOrEqualTo(0);

    // A namespace that does not exist is a 404, which is what the client reads as "no such
    // namespace" without a body to parse.
    resp = client.head(TEST_BASE_PREFIX + "/namespaces/noSuchSchema").aggregate().join();
    assertThat(resp.status().code()).isEqualTo(404);
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

      // A schema Unity Catalog doesn't have is a namespace Iceberg doesn't have. The type matters:
      // Iceberg's client reads it to tell a missing namespace from a missing table.
      assertErrorType(
          client.get(TEST_BASE_PREFIX + "/namespaces/noSuchSchema").aggregate().join(),
          404,
          NoSuchNamespaceException.class);

      // A name Unity Catalog rejects outright, such as the multi-level name a nested namespace
      // arrives as, is a bad request rather than a missing namespace.
      assertErrorType(
          client.get(TEST_BASE_PREFIX + "/namespaces/nested.namespace").aggregate().join(),
          400,
          BadRequestException.class);

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

      // Listing under a catalog Unity Catalog doesn't have is a missing namespace too.
      assertErrorType(
          client.get("/v1/catalogs/noSuchCatalog/namespaces").aggregate().join(),
          404,
          NoSuchNamespaceException.class);

      // non-prefixed URL should result in 404
      resp = client.get(TEST_BASE_NON_PREFIX + "/namespaces").aggregate().join();
      assertThat(resp.status().code()).isEqualTo(404);
    }

    // DropNamespace
    {
      String namespacePath = TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME;

      // A namespace that still holds a table cannot be dropped, and the spec answers that with 409
      // rather than the failed precondition the repository reports.
      createTable(TestUtils.TABLE_NAME);
      AggregatedHttpResponse resp = client.delete(namespacePath).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(409);
      assertThat(ErrorResponseParser.fromJson(resp.contentUtf8()).type())
          .isEqualTo(NamespaceNotEmptyException.class.getSimpleName());

      // Once it is empty the drop answers 204 with no content, and the namespace is gone.
      tableOperations.deleteTable(TestUtils.TABLE_FULL_NAME);
      resp = client.delete(namespacePath).aggregate().join();
      assertThat(resp.status().code()).isEqualTo(204);
      assertThat(resp.contentUtf8()).isEmpty();
      assertThat(client.get(namespacePath).aggregate().join().status().code()).isEqualTo(404);

      // Dropping a namespace that is not there is a 404.
      resp = client.delete(namespacePath).aggregate().join();
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
    assertThat(
            icebergClient.tableExists(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME))
        .isFalse();
    assertErrorType(
        () ->
            icebergClient.loadTable(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME),
        404,
        NoSuchTableException.class);

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
      tableInfoDAO.setIcebergMetadataLocation(metadataLocation);
      session.merge(tableInfoDAO);
      tx.commit();
    }

    // Now the uniform table exists, which the REST spec reports as 204 with no content
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
      assertThat(resp.status().code()).isEqualTo(204);
      assertThat(resp.contentUtf8()).isEmpty();
    }
    // metadata is valid metadata content and metadata location matches
    {
      LoadTableResponse loadTableResponse =
          icebergClient.loadTable(
              TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .isEqualTo(metadataFile.toString());

      // non-prefixed URL should result in 404
      AggregatedHttpResponse resp =
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
      ListTablesResponse listResponse =
          icebergClient.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
      assertThat(listResponse.identifiers())
          .containsExactly(TableIdentifier.of(TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME));

      // non-prefixed URL should result in 404
      AggregatedHttpResponse resp =
          client
              .get(TEST_BASE_NON_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME + "/tables")
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(404);
    }

    // UniForm-derived Iceberg metadata is read-only: commits and drops through the Iceberg REST
    // catalog must be rejected.
    {
      UpdateTableRequest commitRequest =
          new UpdateTableRequest(
              List.of(), List.of(new MetadataUpdate.SetProperties(Map.of("foo", "bar"))));
      assertErrorType(
          () ->
              icebergClient.updateTable(
                  TestUtils.CATALOG_NAME,
                  TestUtils.SCHEMA_NAME,
                  TestUtils.TABLE_NAME,
                  commitRequest),
          400,
          BadRequestException.class);
      assertErrorType(
          () ->
              icebergClient.dropTable(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME),
          400,
          BadRequestException.class);
      assertErrorType(
          () ->
              icebergClient.renameTable(
                  TestUtils.CATALOG_NAME,
                  renameTableRequest(TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, "renamed")),
          400,
          BadRequestException.class);
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
    ApiException conflict =
        assertThrows(
            ApiException.class,
            () ->
                icebergClient.loadTable(
                    TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME));
    assertThat(conflict.getCode()).isEqualTo(400);
    ErrorResponse conflictError = ErrorResponseParser.fromJson(conflict.getResponseBody());
    assertThat(conflictError.type()).isEqualTo(BadRequestException.class.getSimpleName());
    assertThat(conflictError.message()).contains("persisted table location");
  }

  @Test
  public void testIcebergTableWriteLifecycle() throws ApiException, IOException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));

    // Create the namespace through the Iceberg REST catalog
    {
      CreateNamespaceRequest request =
          CreateNamespaceRequest.builder()
              .withNamespace(Namespace.of(TestUtils.SCHEMA_NAME))
              .setProperties(TestUtils.PROPERTIES)
              .build();
      CreateNamespaceResponse createNamespaceResponse =
          icebergClient.createNamespace(TestUtils.CATALOG_NAME, request);
      assertThat(createNamespaceResponse.namespace())
          .isEqualTo(Namespace.of(TestUtils.SCHEMA_NAME));

      // creating it again is a conflict
      assertErrorType(
          () -> icebergClient.createNamespace(TestUtils.CATALOG_NAME, request),
          409,
          AlreadyExistsException.class);
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
      LoadTableResponse loadTableResponse =
          icebergClient.createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation()).isNull();
      TestUtils.assertIcebergApiException(
          () ->
              icebergClient.loadTable(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME),
          404);
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
      LoadTableResponse loadTableResponse =
          icebergClient.createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request);
      initialMetadataLocation = loadTableResponse.tableMetadata().metadataFileLocation();
      assertThat(initialMetadataLocation).contains("/metadata/00000-");
      assertThat(loadTableResponse.tableMetadata().schema().columns()).hasSize(2);
      assertThat(loadTableResponse.tableMetadata().properties())
          .containsEntry("created-by", "iceberg-rest-test");
      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        TableInfoDAO tableInfoDAO = getTableByName(session, TestUtils.TABLE_NAME);
        assertThat(tableInfoDAO.getIcebergMetadataLocation())
            .isEqualTo(NormalizedURL.from(initialMetadataLocation).toString());
      }

      // creating it again is a conflict
      assertErrorType(
          () -> icebergClient.createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request),
          409,
          AlreadyExistsException.class);
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
      assertThat(
              icebergClient.tableExists(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME))
          .isTrue();

      LoadTableResponse loadTableResponse =
          icebergClient.loadTable(
              TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .isEqualTo(initialMetadataLocation);
      tableUuid = loadTableResponse.tableMetadata().uuid();

      ListTablesResponse listTablesResponse =
          icebergClient.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
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
      LoadTableResponse loadTableResponse =
          icebergClient.updateTable(
              TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, request);
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
      LoadTableResponse reloaded =
          icebergClient.loadTable(
              TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
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
      assertErrorType(
          () ->
              icebergClient.updateTable(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, request),
          409,
          CommitFailedException.class);
    }

    // Rename the table, and rename it back so the steps below still find it
    {
      icebergClient.renameTable(
          TestUtils.CATALOG_NAME,
          renameTableRequest(TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, "renamed"));

      // The table answers under its new name and no longer under the old one.
      assertThat(
              icebergClient.tableExists(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "renamed"))
          .isTrue();
      assertThat(
              icebergClient.tableExists(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME))
          .isFalse();
      assertThat(
              icebergClient.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME).identifiers())
          .containsExactly(TableIdentifier.of(Namespace.of(TestUtils.SCHEMA_NAME), "renamed"));

      // A source that is not there is a 404, and a destination that is taken is a 409.
      TestUtils.assertIcebergApiException(
          () ->
              icebergClient.renameTable(
                  TestUtils.CATALOG_NAME,
                  renameTableRequest(TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, "other")),
          404);
      createTable("taken");
      TestUtils.assertIcebergApiException(
          () ->
              icebergClient.renameTable(
                  TestUtils.CATALOG_NAME,
                  renameTableRequest(TestUtils.SCHEMA_NAME, "renamed", "taken")),
          409);

      // Unity Catalog cannot move a table between namespaces, and says so rather than half-doing
      // it.
      TestUtils.assertIcebergApiException(
          () ->
              icebergClient.renameTable(
                  TestUtils.CATALOG_NAME,
                  renameTableRequest(TestUtils.SCHEMA_NAME, "renamed", "moved", "other_ns")),
          501);

      // A request without a source or a destination is a bad request, not a server error. The typed
      // request can't express an empty rename, so this one stays a raw probe.
      assertThat(
              postJson("/v1/catalogs/" + TestUtils.CATALOG_NAME + "/tables/rename", "{}")
                  .status()
                  .code())
          .isEqualTo(400);

      icebergClient.renameTable(
          TestUtils.CATALOG_NAME,
          renameTableRequest(TestUtils.SCHEMA_NAME, "renamed", TestUtils.TABLE_NAME));
    }

    // Drop the table
    {
      icebergClient.dropTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
      assertThat(
              icebergClient.tableExists(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME))
          .isFalse();
      assertErrorType(
          () ->
              icebergClient.loadTable(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME),
          404,
          NoSuchTableException.class);
    }

    // A create request without a location gets a server-assigned managed location
    {
      CreateTableRequest request =
          CreateTableRequest.builder().withName("managed_iceberg_table").withSchema(schema).build();
      LoadTableResponse loadTableResponse =
          icebergClient.createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request);
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

      icebergClient.dropTable(
          TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "managed_iceberg_table");
    }
  }

  @Test
  public void testDropTableAcceptsCapitalizedPurgeRequested() throws Exception {
    createCatalogAndNamespace();
    String tablesPath = TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME + "/tables";

    // Armeria converts a Boolean parameter through a fixed table of true|TRUE|1 and false|FALSE|0,
    // so "True" and "False" -- what pyiceberg sends for a Python bool -- used to be a 400.
    // All of these drop the table: purging is not implemented, so the value itself is not acted on.
    List<String> spellings =
        List.of("true", "false", "True", "False", "TRUE", "FALSE", "tRuE", "1", "0", "");
    for (int i = 0; i < spellings.size(); i++) {
      String spelling = spellings.get(i);
      // A fresh table per spelling, named by position so a mixed-case spelling does not also
      // exercise mixed-case table names.
      String name = "purge_" + i;
      createIcebergTable(tablesPath, name);
      String tablePath = tablesPath + "/" + name;
      AggregatedHttpResponse resp =
          client.delete(tablePath + "?purgeRequested=" + spelling).aggregate().join();
      assertThat(resp.status().code()).as("purgeRequested=%s", spelling).isEqualTo(204);
      assertThat(client.get(tablePath).aggregate().join().status().code())
          .as("loading the table dropped with purgeRequested=%s", spelling)
          .isEqualTo(404);
    }

    // Omitting the parameter stays valid.
    createIcebergTable(tablesPath, "purge_omitted");
    assertThat(client.delete(tablesPath + "/purge_omitted").aggregate().join().status().code())
        .isEqualTo(204);
  }

  @Test
  public void testDropTableRejectsNonBooleanPurgeRequested() throws Exception {
    createCatalogAndNamespace();
    String tablesPath = TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME + "/tables";

    // A value that is not a boolean is refused rather than silently ignored, and the table it named
    // is left in place.
    List<String> rejectedValues = List.of("yes", "purge", "2");
    for (int i = 0; i < rejectedValues.size(); i++) {
      String rejected = rejectedValues.get(i);
      String name = "purge_bad_" + i;
      createIcebergTable(tablesPath, name);
      String tablePath = tablesPath + "/" + name;
      AggregatedHttpResponse resp =
          client.delete(tablePath + "?purgeRequested=" + rejected).aggregate().join();
      assertThat(resp.status().code()).as("purgeRequested=%s", rejected).isEqualTo(400);
      ErrorResponse error = ErrorResponseParser.fromJson(resp.contentUtf8());
      assertThat(error.type()).isEqualTo(BadRequestException.class.getSimpleName());
      assertThat(error.message())
          .isEqualTo("Invalid purgeRequested: " + rejected + ". It must be true or false.");
      assertThat(client.get(tablePath).aggregate().join().status().code())
          .as("loading the table after purgeRequested=%s was refused", rejected)
          .isEqualTo(200);
    }
  }

  private void createCatalogAndNamespace() throws ApiException, IOException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    postJson(
        TEST_BASE_PREFIX + "/namespaces",
        IcebergObjectMapper.mapper()
            .writeValueAsString(
                CreateNamespaceRequest.builder()
                    .withNamespace(Namespace.of(TestUtils.SCHEMA_NAME))
                    .build()));
  }

  private void createIcebergTable(String tablesPath, String name) throws IOException {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    AggregatedHttpResponse resp =
        postJson(
            tablesPath,
            IcebergObjectMapper.mapper()
                .writeValueAsString(
                    CreateTableRequest.builder().withName(name).withSchema(schema).build()));
    assertThat(resp.status().code()).as("creating %s", name).isEqualTo(200);
  }

  @Test
  public void testStagedCreateAndCommit() throws ApiException, IOException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));

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
      staged =
          icebergClient
              .createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request)
              .tableMetadata();
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
              icebergClient
                  .loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME)
                  .tableMetadata()
                  .uuid())
          .isEqualTo(staged.uuid());

      // replaying the create commit loses the race: 409 CommitFailedException
      assertErrorType(
          () ->
              icebergClient.updateTable(
                  TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, request),
          409,
          CommitFailedException.class);
    }

    // staging a create for an existing table is a conflict
    {
      CreateTableRequest request =
          CreateTableRequest.builder()
              .withName(TestUtils.TABLE_NAME)
              .withSchema(schema)
              .stageCreate()
              .build();
      assertErrorType(
          () -> icebergClient.createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request),
          409,
          AlreadyExistsException.class);
    }
  }

  @Test
  public void testConcurrentCommitsSerializeWithCompareAndSwap()
      throws ApiException, IOException, InterruptedException, ExecutionException, TimeoutException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));

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
                barrier.await();
                // The typed client throws on a non-2xx; map it back to the HTTP status so the
                // win/lose bookkeeping below stays status-based.
                try {
                  icebergClient.updateTable(
                      TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, request);
                  return 200;
                } catch (ApiException e) {
                  return e.getCode();
                }
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
    LoadTableResponse loaded =
        icebergClient.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(loaded.tableMetadata().metadataFileLocation()).contains("/metadata/00001-");
    assertThat(loaded.tableMetadata().schema().columns()).hasSize(2);
  }

  @Test
  public void testLoadCredentials() throws Exception {
    createUniformIcebergTable();
    // The route is served: a table UC serves as Iceberg answers 200 with the spec's response. This
    // table is local, so it vends nothing -- an empty list, not a credential with an empty config,
    // which Iceberg's own Credential type rejects.
    assertThat(
            icebergClient
                .loadCredentials(
                    TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME)
                .credentials())
        .isEmpty();

    // A table UC knows about but does not serve as an Iceberg table is a 404, as it is for
    // loadTable, and so is one that does not exist. Both are table-level errors rather than the
    // generic 404 an unrouted path answers, which is how a client can tell this endpoint is served.
    createTable("plainTable");
    assertErrorType(
        () ->
            icebergClient.loadCredentials(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "plainTable"),
        404,
        NoSuchTableException.class);
    assertErrorType(
        () ->
            icebergClient.loadCredentials(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "noSuchTable"),
        404,
        NoSuchTableException.class);
  }

  @Test
  public void testReportMetrics() throws Exception {
    createUniformIcebergTable();

    // Per the REST spec, a report is acknowledged with 204 No Content.
    icebergClient.reportMetrics(
        TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, scanReport());
    icebergClient.reportMetrics(
        TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, commitReport());

    // A body that isn't a metrics report is rejected rather than silently accepted. Iceberg's own
    // parser raises IllegalArgumentException for it, whose name means nothing to a client. The
    // typed
    // client can't send a non-report body, so this stays a raw probe.
    String metricsPath =
        TEST_BASE_PREFIX
            + "/namespaces/"
            + TestUtils.SCHEMA_NAME
            + "/tables/"
            + TestUtils.TABLE_NAME
            + "/metrics";
    assertErrorType(postJson(metricsPath, "{\"foo\":\"bar\"}"), 400, BadRequestException.class);

    // A table UC knows about but doesn't serve as an Iceberg table is a 404, like loadTable.
    createTable("plainTable");
    assertErrorType(
        () ->
            icebergClient.reportMetrics(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "plainTable", scanReport()),
        404,
        NoSuchTableException.class);

    // A table that doesn't exist at all is a 404 too.
    assertErrorType(
        () ->
            icebergClient.reportMetrics(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "missingTable", scanReport()),
        404,
        NoSuchTableException.class);

    // The non-prefixed URL isn't routed, matching the other Iceberg endpoints.
    assertThat(
            postJson(
                    TEST_BASE_NON_PREFIX
                        + "/namespaces/"
                        + TestUtils.SCHEMA_NAME
                        + "/tables/"
                        + TestUtils.TABLE_NAME
                        + "/metrics",
                    scanReportJson())
                .status()
                .code())
        .isEqualTo(404);
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

    ListNamespacesResponse listed = icebergClient.listNamespaces(TestUtils.CATALOG_NAME);
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

    ListTablesResponse listed =
        icebergClient.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
    assertThat(listed.identifiers())
        .containsExactly(TableIdentifier.of(Namespace.of(TestUtils.SCHEMA_NAME), "uniform_table"));
  }

  @Test
  public void testUnroutedIcebergRequestsAnswerWithAnIcebergError() {
    // These paths are answered before any service is reached, so nothing here needs a catalog, a
    // schema or a table to exist.

    // A path the Iceberg API does not serve is answered before any service is reached, so the
    // service's own handler never sees it. The response still has to be an error document Iceberg's
    // parser can read.
    AggregatedHttpResponse resp =
        client
            .get(TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME + "/views")
            .aggregate()
            .join();
    assertIcebergStatusDocument(resp, 404, NotFoundException.class, "Not Found");

    // Same for a method a served path does not accept: the namespaces collection serves listing
    // and creation but not DELETE. Iceberg has no exception of its own for 405.
    resp = client.delete(TEST_BASE_PREFIX + "/namespaces").aggregate().join();
    assertIcebergStatusDocument(resp, 405, RESTException.class, "Method Not Allowed");

    // The mount point itself belongs to the Iceberg API too, even though nothing is served there,
    // so the prefix has to match it as well as the paths under it.
    WebClient rootClient =
        WebClient.builder(serverConfig.getServerUrl())
            .auth(AuthToken.ofOAuth2(serverConfig.getAuthToken()))
            .build();
    resp = rootClient.get("/api/2.1/unity-catalog/iceberg").aggregate().join();
    assertIcebergStatusDocument(resp, 404, NotFoundException.class, "Not Found");

    // Paths outside the Iceberg API keep Armeria's own rendering, whether they are another UC API
    // or the server root.
    AggregatedHttpResponse outside =
        rootClient.get("/api/2.1/unity-catalog/no_such_endpoint").aggregate().join();
    assertThat(outside.status().code()).isEqualTo(404);
    assertThat(outside.contentType()).isNotEqualTo(MediaType.JSON);

    outside = rootClient.get("/").aggregate().join();
    assertThat(outside.status().code()).isEqualTo(200);
    assertThat(outside.contentUtf8()).isEqualTo("Hello, Unity Catalog!");
  }

  /**
   * Asserts the response is the Iceberg error document a status raised before any service was
   * reached has to be rendered as: Iceberg's own media type, the status in the body as well as on
   * the response, and a type Iceberg's client knows.
   */
  private static void assertIcebergStatusDocument(
      AggregatedHttpResponse resp,
      int expectedCode,
      Class<?> expectedType,
      String expectedMessage) {
    assertThat(resp.status().code()).isEqualTo(expectedCode);
    assertThat(resp.contentType()).isEqualTo(MediaType.JSON);
    ErrorResponse error = ErrorResponseParser.fromJson(resp.contentUtf8());
    assertThat(error.code()).isEqualTo(expectedCode);
    assertThat(error.type()).isEqualTo(expectedType.getSimpleName());
    assertThat(error.message()).isEqualTo(expectedMessage);
  }

  /**
   * Asserts the response is an Iceberg error whose type is the given Iceberg exception. The type is
   * what Iceberg's client reads to decide which exception to raise, so it has to be a name the
   * client knows, never an internal Unity Catalog one.
   */
  private static void assertErrorType(
      AggregatedHttpResponse resp, int expectedCode, Class<? extends Exception> expectedType) {
    assertThat(resp.status().code()).isEqualTo(expectedCode);
    ErrorResponse error = ErrorResponseParser.fromJson(resp.contentUtf8());
    assertThat(error.type()).isEqualTo(expectedType.getSimpleName());
    assertThat(error.code()).isEqualTo(expectedCode);
  }

  /**
   * As {@link #assertErrorType} but for a typed {@link IcebergRestClient} call, which throws {@link
   * ApiException} carrying the Iceberg error body on a non-2xx response.
   */
  private static void assertErrorType(
      Executable call, int expectedCode, Class<? extends Exception> expectedType) {
    ApiException e = assertThrows(ApiException.class, call);
    assertThat(e.getCode()).isEqualTo(expectedCode);
    ErrorResponse error = ErrorResponseParser.fromJson(e.getResponseBody());
    assertThat(error.type()).isEqualTo(expectedType.getSimpleName());
    assertThat(error.code()).isEqualTo(expectedCode);
  }

  private static RenameTableRequest renameTableRequest(String namespace, String from, String to) {
    return renameTableRequest(namespace, from, to, namespace);
  }

  private static RenameTableRequest renameTableRequest(
      String namespace, String from, String to, String destinationNamespace) {
    return RenameTableRequest.builder()
        .withSource(TableIdentifier.of(Namespace.of(namespace), from))
        .withDestination(TableIdentifier.of(Namespace.of(destinationNamespace), to))
        .build();
  }

  @Test
  public void testLoadTableSnapshotsParameter() throws Exception {
    createUniformIcebergTable("/iceberg.metadata.two-snapshots.json");
    // The default is every snapshot the metadata holds, which includes the one no ref points at.
    assertThat(
            loadedSnapshotIds(
                icebergClient.loadTable(
                    TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME)))
        .hasSize(2);
    assertThat(
            loadedSnapshotIds(
                icebergClient.loadTable(
                    TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, "all")))
        .hasSize(2);

    // "refs" asks for only the snapshots the table's refs point at.
    LoadTableResponse loaded =
        icebergClient.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, "refs");
    assertThat(loadedSnapshotIds(loaded))
        .containsExactly(loaded.tableMetadata().currentSnapshot().snapshotId());
    // The rest of the metadata is the same table, so a client can still use what it got back.
    assertThat(loaded.tableMetadata().metadataFileLocation())
        .isEqualTo(
            icebergClient
                .loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME)
                .tableMetadata()
                .metadataFileLocation());

    // Any other value is a bad request rather than a silently complete response.
    assertErrorType(
        () ->
            icebergClient.loadTable(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME, "some"),
        400,
        BadRequestException.class);
  }

  @Test
  public void testLoadTableConditionalGet() throws Exception {
    createUniformIcebergTable("/iceberg.metadata.two-snapshots.json");
    String tablePath =
        TEST_BASE_PREFIX
            + "/namespaces/"
            + TestUtils.SCHEMA_NAME
            + "/tables/"
            + TestUtils.TABLE_NAME;

    AggregatedHttpResponse loaded = client.get(tablePath).aggregate().join();
    assertThat(loaded.status().code()).isEqualTo(200);
    String etag = loaded.headers().get(HttpHeaderNames.ETAG);
    // Weak, because the tag names the metadata version rather than the bytes of one response.
    assertThat(etag).startsWith("W/\"").endsWith("\"");
    // The tag must not hand out the metadata file location.
    assertThat(etag).doesNotContain("iceberg.metadata");

    // A caller that already holds this version gets a bodiless 304.
    AggregatedHttpResponse notModified = conditionalGet(tablePath, etag);
    assertThat(notModified.status().code()).isEqualTo(304);
    assertThat(notModified.content().isEmpty()).isTrue();
    assertThat(notModified.headers().get(HttpHeaderNames.ETAG)).isEqualTo(etag);

    // The header may list several tags; ours being one of them is enough.
    assertThat(conditionalGet(tablePath, "W/\"0\", " + etag).status().code()).isEqualTo(304);

    // A tag from some other version, and the wildcard, both get the full response.
    assertThat(conditionalGet(tablePath, "W/\"0\"").status().code()).isEqualTo(200);
    assertThat(conditionalGet(tablePath, "*").status().code()).isEqualTo(200);

    // "refs" returns a different body for the same version, so it carries its own tag and is not
    // answered from a copy the default response handed out.
    String refsPath = tablePath + "?snapshots=refs";
    String refsETag = client.get(refsPath).aggregate().join().headers().get(HttpHeaderNames.ETAG);
    assertThat(refsETag).isNotEqualTo(etag);
    assertThat(conditionalGet(refsPath, etag).status().code()).isEqualTo(200);
    assertThat(conditionalGet(refsPath, refsETag).status().code()).isEqualTo(304);

    // An invalid snapshots value is still rejected rather than answered from the caller's copy.
    assertThat(conditionalGet(tablePath + "?snapshots=some", etag).status().code()).isEqualTo(400);

    // Moving the metadata pointer, as a commit does, retires the tag the caller holds.
    TableInfo table =
        tableOperations.getTable(
            TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + TestUtils.TABLE_NAME);
    Path committed =
        Files.copy(
            icebergTableLocation.resolve("iceberg.metadata.json"),
            icebergTableLocation.resolve("committed.metadata.json"));
    setUniformMetadata(table, committed);

    AggregatedHttpResponse reloaded = conditionalGet(tablePath, etag);
    assertThat(reloaded.status().code()).isEqualTo(200);
    assertThat(reloaded.headers().get(HttpHeaderNames.ETAG)).isNotEqualTo(etag);

    // Released Iceberg clients keep response headers in a map keyed by the name as received and
    // look this one up as "ETag", so the name has to reach the wire spelled that way.
    assertThat(responseHead(tablePath)).contains("ETag: W/\"");
  }

  /**
   * Reads a response head straight off the socket. Both Armeria's client and the JDK's lowercase
   * header names as they parse them, so neither can see how the server spelled them.
   */
  private String responseHead(String path) throws IOException {
    URI server = URI.create(serverConfig.getServerUrl());
    try (Socket socket = new Socket(server.getHost(), server.getPort())) {
      socket
          .getOutputStream()
          .write(
              ("GET "
                      + ICEBERG_BASE_PATH
                      + path
                      + " HTTP/1.1\r\nHost: "
                      + server.getAuthority()
                      + "\r\nAuthorization: Bearer "
                      + serverConfig.getAuthToken()
                      + "\r\nConnection: close\r\n\r\n")
                  .getBytes(StandardCharsets.UTF_8));
      String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      int endOfHead = response.indexOf("\r\n\r\n");
      return endOfHead < 0 ? response : response.substring(0, endOfHead);
    }
  }

  private AggregatedHttpResponse conditionalGet(String path, String ifNoneMatch) {
    return client
        .execute(
            RequestHeaders.of(HttpMethod.GET, path, HttpHeaderNames.IF_NONE_MATCH, ifNoneMatch))
        .aggregate()
        .join();
  }

  private static List<Long> loadedSnapshotIds(LoadTableResponse loaded) {
    return loaded.tableMetadata().snapshots().stream().map(Snapshot::snapshotId).toList();
  }

  @Test
  public void testUpdateNamespaceProperties() throws Exception {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema()
            .catalogName(TestUtils.CATALOG_NAME)
            .name(TestUtils.SCHEMA_NAME)
            .properties(Map.of("keep", "me", "drop", "this")));
    String propertiesPath =
        TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME + "/properties";

    // What was set, what was removed, and what could not be removed because it was not there.
    AggregatedHttpResponse resp =
        postJson(
            propertiesPath,
            "{\"removals\": [\"drop\", \"absent\"], \"updates\": {\"added\": \"value\"}}");
    assertThat(resp.status().code()).isEqualTo(200);
    UpdateNamespacePropertiesResponse updated =
        IcebergObjectMapper.mapper()
            .readValue(resp.contentUtf8(), UpdateNamespacePropertiesResponse.class);
    assertThat(updated.updated()).containsExactly("added");
    assertThat(updated.removed()).containsExactly("drop");
    assertThat(updated.missing()).containsExactly("absent");

    // The namespace itself reflects the patch: keys not mentioned are left alone.
    assertThat(namespaceProperties(TestUtils.SCHEMA_NAME))
        .containsOnly(Map.entry("keep", "me"), Map.entry("added", "value"));

    // Removing every key leaves the namespace with none. Worth pinning separately: a patch that
    // ends in an empty property set is what a later "nothing to write" shortcut would get wrong.
    resp = postJson(propertiesPath, "{\"removals\": [\"keep\", \"added\"], \"updates\": {}}");
    assertThat(resp.status().code()).isEqualTo(200);
    updated =
        IcebergObjectMapper.mapper()
            .readValue(resp.contentUtf8(), UpdateNamespacePropertiesResponse.class);
    assertThat(updated.removed()).containsExactlyInAnyOrder("keep", "added");
    assertThat(updated.updated()).isEmpty();
    assertThat(namespaceProperties(TestUtils.SCHEMA_NAME)).isEmpty();

    // A key both set and removed is the spec's 422, and a namespace that does not exist is a 404.
    resp =
        postJson(propertiesPath, "{\"removals\": [\"both\"], \"updates\": {\"both\": \"value\"}}");
    assertThat(resp.status().code()).isEqualTo(422);
    assertThat(ErrorResponseParser.fromJson(resp.contentUtf8()).type())
        .isEqualTo(UnprocessableEntityException.class.getSimpleName());
    resp =
        postJson(
            TEST_BASE_PREFIX + "/namespaces/noSuchSchema/properties",
            "{\"removals\": [], \"updates\": {\"a\": \"b\"}}");
    assertThat(resp.status().code()).isEqualTo(404);
  }

  /** The properties the namespace reports, as a client reading it back would see them. */
  private Map<String, String> namespaceProperties(String namespace) throws IOException {
    AggregatedHttpResponse resp =
        client.get(TEST_BASE_PREFIX + "/namespaces/" + namespace).aggregate().join();
    assertThat(resp.status().code()).isEqualTo(200);
    return IcebergObjectMapper.mapper()
        .readValue(resp.contentUtf8(), GetNamespaceResponse.class)
        .properties();
  }

  @Test
  public void testListNamespacesUnderAParent() throws ApiException, IOException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));

    // A namespace that exists has no children, because Unity Catalog has no nested namespaces.
    AggregatedHttpResponse resp =
        client
            .get(TEST_BASE_PREFIX + "/namespaces?parent=" + TestUtils.SCHEMA_NAME)
            .aggregate()
            .join();
    assertThat(resp.status().code()).isEqualTo(200);
    assertThat(
            IcebergObjectMapper.mapper()
                .readValue(resp.contentUtf8(), ListNamespacesResponse.class)
                .namespaces())
        .isEmpty();

    // A parent that does not exist is a 404, not an empty listing: the client has to be able to
    // tell "this namespace has no children" from "there is no such namespace".
    resp = client.get(TEST_BASE_PREFIX + "/namespaces?parent=noSuchSchema").aggregate().join();
    assertThat(resp.status().code()).isEqualTo(404);
    assertThat(ErrorResponseParser.fromJson(resp.contentUtf8()).type())
        .isEqualTo(NoSuchNamespaceException.class.getSimpleName());

    // An empty parent is not a parent at all, so the listing is the whole one.
    resp = client.get(TEST_BASE_PREFIX + "/namespaces?parent=").aggregate().join();
    assertThat(resp.status().code()).isEqualTo(200);
    assertThat(
            IcebergObjectMapper.mapper()
                .readValue(resp.contentUtf8(), ListNamespacesResponse.class)
                .namespaces())
        .containsExactly(Namespace.of(TestUtils.SCHEMA_NAME));

    // A catalog that does not exist is a 404 whether or not a parent is asked for.
    resp =
        client
            .get("/v1/catalogs/noSuchCatalog/namespaces?parent=" + TestUtils.SCHEMA_NAME)
            .aggregate()
            .join();
    assertThat(resp.status().code()).isEqualTo(404);
  }

  @Test
  public void testReportMetricsRejectsAnUnreadableBodyWithoutLeakingTheParser() throws Exception {
    createUniformIcebergTable();
    String metricsPath =
        TEST_BASE_PREFIX
            + "/namespaces/"
            + TestUtils.SCHEMA_NAME
            + "/tables/"
            + TestUtils.TABLE_NAME
            + "/metrics";

    // A body that is missing, one that is not JSON at all, and one that is JSON the endpoint cannot
    // map are all rejected as bad requests named in Iceberg's vocabulary rather than the
    // converter's, and each says which of the three it was.
    assertUnreadableBody(postJson(metricsPath, ""), "Malformed request body: no content");
    assertUnreadableBody(postJson(metricsPath, "{"), "Malformed request body: not valid JSON");
    // The mapped shape is where the reader would otherwise name the Java type it was mapping onto.
    assertUnreadableBody(
        postJson(TEST_BASE_PREFIX + "/namespaces", "{\"namespace\": [\"x\"], \"properties\": 5}"),
        "Malformed request body: not the structure this endpoint accepts");
  }

  /**
   * Asserts the response rejects the body as unreadable without quoting the JSON reader: neither
   * the parser's own classes, nor the Java types it was mapping the body onto, nor the location it
   * had reached in the body belong in an error a client is shown.
   */
  private static void assertUnreadableBody(AggregatedHttpResponse resp, String expectedMessage) {
    assertThat(resp.status().code()).isEqualTo(400);
    ErrorResponse error = ErrorResponseParser.fromJson(resp.contentUtf8());
    assertThat(error.code()).isEqualTo(400);
    assertThat(error.type()).isEqualTo(BadRequestException.class.getSimpleName());
    assertThat(error.message()).isEqualTo(expectedMessage);
  }

  @Test
  public void testLoadTableWhoseMetadataCannotBeRead() throws Exception {
    createUniformIcebergTable();
    // The catalog still lists the table; the file its metadata pointer names is gone.
    Files.delete(icebergTableLocation.resolve("iceberg.metadata.json"));

    ApiException e =
        assertThrows(
            ApiException.class,
            () ->
                icebergClient.loadTable(
                    TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME));

    // A table whose metadata cannot be read is a server-side failure, not a missing table, so the
    // type has to be one that means that: a 500 typed "NotFoundException" describes the failure as
    // something the client could act on.
    assertThat(e.getCode()).isEqualTo(500);
    ErrorResponse error = ErrorResponseParser.fromJson(e.getResponseBody());
    assertThat(error.code()).isEqualTo(500);
    assertThat(error.type()).isEqualTo(ServiceFailureException.class.getSimpleName());
    // The message says the table could not be read, and where the server keeps its files is not the
    // client's business.
    assertThat(error.message()).isEqualTo("Could not read this table");
    assertThat(error.message()).doesNotContain(icebergTableLocation.toString());
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

  private static String scanReportJson() {
    return ReportMetricsRequestParser.toJson(scanReport());
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

  /** Creates a table that the Iceberg endpoints see, i.e. one with uniform Iceberg metadata. */
  private void createUniformIcebergTable() throws IOException, URISyntaxException, ApiException {
    createUniformIcebergTable("/iceberg.metadata.json");
  }

  private void createUniformIcebergTable(String metadataResource)
      throws IOException, URISyntaxException, ApiException {
    Path metadataFile = writeIcebergMetadata(metadataResource);
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
      tableInfoDAO.setIcebergMetadataLocation(metadataFile.toUri().toString());
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
    return writeIcebergMetadata("/iceberg.metadata.json");
  }

  private Path writeIcebergMetadata(String resource) throws IOException, URISyntaxException {
    Path source = Path.of(Objects.requireNonNull(this.getClass().getResource(resource)).toURI());
    Path metadataFile = icebergTableLocation.resolve("iceberg.metadata.json");
    String tableLocation = NormalizedURL.from(icebergTableLocation.toUri()).toString();
    String metadata =
        Files.readString(source).replace("file:/tmp/uniform_iceberg_table", tableLocation);
    return Files.writeString(metadataFile, metadata);
  }
}
