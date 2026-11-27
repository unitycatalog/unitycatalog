package io.unitycatalog.server.sdk.tables;

import static io.unitycatalog.server.utils.TestUtils.assertHttpApiException;
import static io.unitycatalog.server.utils.TestUtils.sendRawGet;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Exercises the table existence contract through the generated client and HTTP server. */
public class SdkTableExistsTest extends BaseTableCRUDTestEnv {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String TABLES_PATH = "/api/2.1/unity-catalog/tables/";

  private TablesApi tablesApi;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig config) {
    tablesApi = new TablesApi(TestUtils.createApiClient(config));
    return new SdkTableOperations(TestUtils.createApiClient(config));
  }

  @Test
  void tableExistsTracksPersistedManagedAndExternalTables() throws Exception {
    for (String missingName :
        List.of(
            "missing_catalog.missing_schema.missing_table",
            TestUtils.CATALOG_NAME + ".missing_schema.missing_table",
            TestUtils.TABLE_FULL_NAME)) {
      assertExists(missingName, false);
    }

    tablesApi.createStagingTable(
        new CreateStagingTable()
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .name("staging_only"));
    assertExists(TestUtils.SCHEMA_FULL_NAME + ".staging_only", false);

    for (TableType tableType : List.of(TableType.EXTERNAL, TableType.MANAGED)) {
      TableInfo created =
          createTestingTable(
              TestUtils.TABLE_NAME,
              tableType,
              tableType == TableType.EXTERNAL
                  ? Optional.of(Files.createTempDirectory(testDirectoryRoot, "external").toString())
                  : Optional.empty(),
              tableType == TableType.EXTERNAL ? DataSourceFormat.CSV : DataSourceFormat.DELTA,
              tableOperations);
      assertThat(tableOperations.getTable(TestUtils.TABLE_FULL_NAME).getTableId())
          .isEqualTo(created.getTableId());
      assertExists(TestUtils.TABLE_FULL_NAME, true);
      tableOperations.deleteTable(TestUtils.TABLE_FULL_NAME);
      assertExists(TestUtils.TABLE_FULL_NAME, false);
    }
  }

  @Test
  void tableExistsRejectsMalformedNames() throws Exception {
    for (String encodedName :
        List.of(
            "table",
            "catalog.table",
            "catalog.schema.table.extra",
            ".schema.table",
            "catalog..table",
            "catalog.schema.",
            "catalog.schema.bad%20name",
            "catalog.schema.bad%2Fname",
            "catalog.schema." + "a".repeat(256))) {
      assertHttpApiException(
          sendRawGet(serverConfig, TABLES_PATH + encodedName + "/exists"),
          ErrorCode.INVALID_ARGUMENT);
    }

    // Armeria rejects encoded control characters before routing to the service.
    for (String encodedControl : List.of("%00", "%0A")) {
      HttpResponse<String> response =
          sendRawGet(
              serverConfig, TABLES_PATH + "catalog.schema.bad" + encodedControl + "name/exists");
      assertThat(response.statusCode()).isEqualTo(400);
      assertThat(response.body()).isEqualTo("Status: 400\nDescription: Invalid request path\n");
    }
  }

  private void assertExists(String fullName, boolean expected) throws Exception {
    assertThat(tablesApi.tableExists(fullName).getTableExists()).isEqualTo(expected);
    HttpResponse<String> response = sendRawGet(serverConfig, TABLES_PATH + fullName + "/exists");
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(MAPPER.readTree(response.body()))
        .isEqualTo(MAPPER.createObjectNode().put("table_exists", expected));
  }
}
