package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
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
import io.unitycatalog.server.utils.RESTObjectMapper;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
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
                + "\"}}");

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
              RESTObjectMapper.mapper().readValue(resp.contentUtf8(), GetNamespaceResponse.class))
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
              RESTObjectMapper.mapper().readValue(resp.contentUtf8(), ListNamespacesResponse.class))
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
            .typeJson("{\"type\": \"integer\"}")
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
            .typeJson("{\"type\": \"string\", \"length\": \"255\"}")
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
          RESTObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
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
          RESTObjectMapper.mapper().readValue(resp.contentUtf8(), ListTablesResponse.class);
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
  }
}
