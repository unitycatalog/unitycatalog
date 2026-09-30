package io.unitycatalog.server.observability;

import static io.unitycatalog.server.utils.TestUtils.sendRawGet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.delta.DeltaBaseTableCRUDTestEnv;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.service.iceberg.IcebergObjectMapper;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Exercises health, port isolation, and persisted-create metrics against one UC server. */
public class ObservabilityEndpointsIntegrationTest extends DeltaBaseTableCRUDTestEnv {

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.OBSERVABILITY_ENABLED.getKey(), "true");
    serverProperties.setProperty(Property.ICEBERG_TABLE_ENABLED.getKey(), "true");
  }

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
    return new SdkTableOperations(TestUtils.createApiClient(config));
  }

  @Test
  public void observabilityEndpointsAndPersistedCreates() throws Exception {
    assertHealthAndPortIsolation();
    ucRestCountsOnlySuccessfullyPersistedCreates();
    deltaRestCountsOnlySuccessfullyPersistedCreates();
    icebergRestCountsOnlySuccessfullyPersistedCreates();
    for (TableType tableType : List.of(TableType.VIEW, TableType.METRIC_VIEW)) {
      viewLikeRestCountsOnlySuccessfullyPersistedCreates(tableType);
    }
  }

  private void assertHealthAndPortIsolation() throws Exception {
    // The synchronous startup probe has already checked the H2 database.
    for (String path : List.of("/livez", "/readyz")) {
      HttpResponse<String> response = sendRawGet(observabilityServerConfig, path);
      assertThat(response.statusCode()).as(path).isEqualTo(200);
      assertThat(response.body()).contains("\"healthy\":true");
    }

    for (String path : List.of("/livez", "/readyz", "/metrics")) {
      assertThat(sendRawGet(serverConfig, path).statusCode()).as(path).isEqualTo(404);
    }

    HttpResponse<String> apiRoot = sendRawGet(serverConfig, "/");
    assertThat(apiRoot.statusCode()).isEqualTo(200);
    assertThat(apiRoot.body()).contains("Hello, Unity Catalog!");
    for (String path : List.of("/", "/docs", "/api/2.1/unity-catalog/catalogs")) {
      assertThat(sendRawGet(observabilityServerConfig, path).statusCode()).as(path).isEqualTo(404);
    }
  }

  private void ucRestCountsOnlySuccessfullyPersistedCreates() throws Exception {
    double before = tablesCreated();
    createAndVerifyExternalTable();
    assertThat(tablesCreated()).isEqualTo(before + 1.0);

    assertThatThrownBy(this::createAndVerifyExternalTable).hasMessageContaining("already exists");
    assertThat(tablesCreated()).isEqualTo(before + 1.0);
  }

  private void deltaRestCountsOnlySuccessfullyPersistedCreates() throws Exception {
    double before = tablesCreated();
    createDeltaExternal("delta_metric_table");
    assertThat(tablesCreated()).isEqualTo(before + 1.0);

    assertThatThrownBy(() -> createDeltaExternal("delta_metric_table"))
        .hasMessageContaining("already exists");
    assertThat(tablesCreated()).isEqualTo(before + 1.0);
  }

  private void icebergRestCountsOnlySuccessfullyPersistedCreates() throws Exception {
    String tablesPath =
        "/api/2.1/unity-catalog/iceberg/v1/catalogs/"
            + TestUtils.CATALOG_NAME
            + "/namespaces/"
            + TestUtils.SCHEMA_NAME
            + "/tables";
    String location =
        Files.createTempDirectory(testDirectoryRoot, "iceberg_metric_").toUri().toString();
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName("iceberg_metric_table")
            .withSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .withLocation(location)
            .build();
    String body = IcebergObjectMapper.mapper().writeValueAsString(request);

    double before = tablesCreated();
    HttpResponse<String> created =
        TestUtils.sendRaw(serverConfig, "POST", tablesPath, Optional.of(body));
    assertThat(created.statusCode()).as(created.body()).isEqualTo(200);
    assertThat(tablesCreated()).isEqualTo(before + 1.0);

    HttpResponse<String> duplicate =
        TestUtils.sendRaw(serverConfig, "POST", tablesPath, Optional.of(body));
    assertThat(duplicate.statusCode()).as(duplicate.body()).isEqualTo(409);
    assertThat(tablesCreated()).isEqualTo(before + 1.0);
  }

  private void viewLikeRestCountsOnlySuccessfullyPersistedCreates(TableType tableType)
      throws Exception {
    String sourceName = tableType.name().toLowerCase(Locale.ROOT) + "_metric_source_table";
    createTestingTable(
        sourceName,
        TableType.EXTERNAL,
        Optional.of(Files.createTempDirectory(testDirectoryRoot, "metric_source_").toString()),
        tableOperations);
    String sourceFullName = TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + sourceName;
    DependencyList dependencies =
        new DependencyList()
            .dependencies(
                List.of(
                    new Dependency().table(new TableDependency().tableFullName(sourceFullName))));
    CreateTable request =
        new CreateTable()
            .name(tableType.name().toLowerCase(Locale.ROOT) + "_metric_table")
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(COLUMNS)
            .tableType(tableType)
            .viewDefinition(
                tableType == TableType.VIEW
                    ? "SELECT * FROM " + sourceFullName
                    : "version: \"0.1\"\nsource: " + sourceFullName)
            .viewDependencies(dependencies);

    double before = tablesCreated();
    tableOperations.createTable(request);
    assertThat(tablesCreated()).isEqualTo(before + 1.0);

    assertThatThrownBy(() -> tableOperations.createTable(request))
        .hasMessageContaining("already exists");
    assertThat(tablesCreated()).isEqualTo(before + 1.0);
  }

  private double tablesCreated() throws Exception {
    HttpResponse<String> response = sendRawGet(observabilityServerConfig, "/metrics");

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body())
        .contains("jvm_memory_used_bytes", "uc_securable_table_created", "http_server_");

    return response
        .body()
        .lines()
        .filter(line -> line.startsWith("uc_securable_table_created_total"))
        .mapToDouble(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
        .findFirst()
        .orElse(0.0);
  }
}
