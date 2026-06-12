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
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.service.iceberg.IcebergObjectMapper;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
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

public class IcebergRestCatalogTest extends BaseServerTest {

  private static final String TEST_BASE_PREFIX = "/v1/catalogs/" + TestUtils.CATALOG_NAME;
  private static final String TEST_BASE_NON_PREFIX = "/v1";

  protected CatalogOperations catalogOperations;
  protected SchemaOperations schemaOperations;
  protected TableOperations tableOperations;
  private WebClient client;

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
  public void testTable() throws ApiException, IOException, URISyntaxException {
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
            .storageLocation("/tmp/stagingLocation")
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

    // Add the uniform metadata
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      Transaction tx = session.beginTransaction();
      TableInfoDAO tableInfoDAO = TableInfoDAO.builder().build();
      assertThat(tableInfo.getTableId()).isNotNull();
      session.load(tableInfoDAO, UUID.fromString(tableInfo.getTableId()));
      String metadataLocation =
          Objects.requireNonNull(this.getClass().getResource("/iceberg.metadata.json"))
              .toURI()
              .toString();
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
          .isEqualTo(
              Objects.requireNonNull(this.getClass().getResource("/iceberg.metadata.json"))
                  .getPath());

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
    String location =
        java.nio.file.Files.createTempDirectory("iceberg-rest-table").toUri().toString();

    // Staged creation is rejected with a clear error
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
      assertThat(resp.status().code()).isEqualTo(400);
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
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse loadTableResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      initialMetadataLocation = loadTableResponse.tableMetadata().metadataFileLocation();
      assertThat(initialMetadataLocation).contains("/metadata/00000-");
      assertThat(loadTableResponse.tableMetadata().schema().columns()).hasSize(2);
      assertThat(loadTableResponse.tableMetadata().properties())
          .containsEntry("created-by", "iceberg-rest-test");

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
      UpdateTableRequest request =
          new UpdateTableRequest(
              List.of(new UpdateRequirement.AssertTableUUID(tableUuid)),
              List.of(new MetadataUpdate.SetProperties(Map.of("foo", "bar"))));
      AggregatedHttpResponse resp =
          postJson(tablePath, IcebergObjectMapper.mapper().writeValueAsString(request));
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse loadTableResponse =
          IcebergObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      assertThat(loadTableResponse.tableMetadata().metadataFileLocation())
          .contains("/metadata/00001-");
      assertThat(loadTableResponse.tableMetadata().properties()).containsEntry("foo", "bar");

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

  private AggregatedHttpResponse postJson(String path, String body) {
    return client
        .execute(
            RequestHeaders.builder(HttpMethod.POST, path).contentType(MediaType.JSON).build(), body)
        .aggregate()
        .join();
  }
}
