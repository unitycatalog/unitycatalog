package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.auth.AuthToken;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.RESTObjectMapper;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for Iceberg auto-resolve feature.
 */
public class IcebergAutoResolveTest extends BaseServerTest {

  private static final String ICEBERG_TABLE_NAME = "uc_iceberg_auto";
  private static final String TEST_BASE_PREFIX = "/v1/catalogs/" + TestUtils.CATALOG_NAME;

  private CatalogOperations catalogOperations;
  private SchemaOperations schemaOperations;
  private TableOperations tableOperations;
  private WebClient client;
  private Path icebergTableRoot;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(
        ServerProperties.Property.ICEBERG_AUTO_RESOLVE_ENABLED.getKey(), "true");
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

  private void cleanUp() {
    try {
      if (catalogOperations.getCatalog(TestUtils.CATALOG_NAME) != null) {
        catalogOperations.deleteCatalog(TestUtils.CATALOG_NAME, Optional.of(true));
      }
    } catch (Exception e) {
    }
  }

  private void setupIcebergMetadata(int version) throws IOException {
    icebergTableRoot = testDirectoryRoot.resolve("iceberg_table");
    Path metadataDir = icebergTableRoot.resolve("metadata");
    Files.createDirectories(metadataDir);

    try (InputStream is =
        Objects.requireNonNull(getClass().getResourceAsStream("/iceberg.metadata.json"))) {
      String metadataContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      Files.writeString(metadataDir.resolve("v" + version + ".metadata.json"), metadataContent);
    }

    Files.writeString(metadataDir.resolve("version-hint.text"), String.valueOf(version));
  }

  @Test
  public void testIcebergAutoResolve() throws ApiException, IOException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));
    setupIcebergMetadata(1);

    ColumnInfo columnInfo =
        new ColumnInfo()
            .name("x")
            .typeText("integer")
            .typeJson("{\"type\": \"integer\"}")
            .typeName(ColumnTypeName.INT)
            .position(0)
            .nullable(true);
    CreateTable createTableRequest =
        new CreateTable()
            .name(ICEBERG_TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(List.of(columnInfo))
            .storageLocation(icebergTableRoot.toUri().toString())
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.ICEBERG);
    tableOperations.createTable(createTableRequest);

    {
      AggregatedHttpResponse resp =
          client
              .head(
                  TEST_BASE_PREFIX
                      + "/namespaces/"
                      + TestUtils.SCHEMA_NAME
                      + "/tables/"
                      + ICEBERG_TABLE_NAME)
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(200);
    }

    {
      AggregatedHttpResponse resp =
          client
              .get(
                  TEST_BASE_PREFIX
                      + "/namespaces/"
                      + TestUtils.SCHEMA_NAME
                      + "/tables/"
                      + ICEBERG_TABLE_NAME)
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse loadTableResponse =
          RESTObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      assertThat(loadTableResponse.tableMetadata()).isNotNull();
      assertThat(loadTableResponse.tableMetadata().uuid().toString())
          .isEqualTo("55d4dc69-5b14-4483-bfc8-f33b80f99f99");
    }

    {
      AggregatedHttpResponse resp =
          client
              .get(TEST_BASE_PREFIX + "/namespaces/" + TestUtils.SCHEMA_NAME + "/tables")
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(200);
      ListTablesResponse listResp =
          RESTObjectMapper.mapper().readValue(resp.contentUtf8(), ListTablesResponse.class);
      assertThat(listResp.identifiers())
          .contains(TableIdentifier.of(Namespace.of(TestUtils.SCHEMA_NAME), ICEBERG_TABLE_NAME));
    }
  }

  @Test
  public void testAutoResolvePicksUpNewVersion() throws ApiException, IOException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));
    setupIcebergMetadata(1);

    ColumnInfo columnInfo =
        new ColumnInfo()
            .name("x")
            .typeText("integer")
            .typeJson("{\"type\": \"integer\"}")
            .typeName(ColumnTypeName.INT)
            .position(0)
            .nullable(true);
    tableOperations.createTable(
        new CreateTable()
            .name(ICEBERG_TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(List.of(columnInfo))
            .storageLocation(icebergTableRoot.toUri().toString())
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.ICEBERG));

    {
      AggregatedHttpResponse resp =
          client
              .get(
                  TEST_BASE_PREFIX
                      + "/namespaces/"
                      + TestUtils.SCHEMA_NAME
                      + "/tables/"
                      + ICEBERG_TABLE_NAME)
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse loadResp =
          RESTObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      assertThat(loadResp.tableMetadata().metadataFileLocation()).contains("v1.metadata.json");
    }

    Path metadataDir = icebergTableRoot.resolve("metadata");
    Files.copy(metadataDir.resolve("v1.metadata.json"), metadataDir.resolve("v2.metadata.json"));
    Files.writeString(metadataDir.resolve("version-hint.text"), "2");

    {
      AggregatedHttpResponse resp =
          client
              .get(
                  TEST_BASE_PREFIX
                      + "/namespaces/"
                      + TestUtils.SCHEMA_NAME
                      + "/tables/"
                      + ICEBERG_TABLE_NAME)
              .aggregate()
              .join();
      assertThat(resp.status().code()).isEqualTo(200);
      LoadTableResponse loadResp =
          RESTObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      assertThat(loadResp.tableMetadata().metadataFileLocation()).contains("v2.metadata.json");
    }
  }

  @Test
  public void testDeltaTableUnaffectedByAutoResolve() throws ApiException, IOException {
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));

    ColumnInfo columnInfo =
        new ColumnInfo()
            .name("id")
            .typeText("integer")
            .typeJson("{\"type\": \"integer\"}")
            .typeName(ColumnTypeName.INT)
            .position(0)
            .nullable(true);
    tableOperations.createTable(
        new CreateTable()
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(List.of(columnInfo))
            .storageLocation("/tmp/some_delta_table")
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA));

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
}
