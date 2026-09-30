package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
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
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * End-to-end integration tests for the {@code uc_tables_created} counter exported on the live
 * {@code /metrics} scrape endpoint.
 */
public class MetricsDomainCounterTest extends DeltaBaseTableCRUDTestEnv {

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
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
  @SneakyThrows
  public void ucRestCountsOnlySuccessfullyPersistedCreates() {
    double before = tablesCreated();
    createAndVerifyExternalTable();
    assertThat(tablesCreated()).isEqualTo(before + 1.0);

    assertThatThrownBy(this::createAndVerifyExternalTable).hasMessageContaining("already exists");
    assertThat(tablesCreated()).isEqualTo(before + 1.0);
  }

  @Test
  public void deltaRestCountsOnlySuccessfullyPersistedCreates() {
    double before = tablesCreated();
    createDeltaExternal("delta_metric_table");
    assertThat(tablesCreated()).isEqualTo(before + 1.0);

    assertThatThrownBy(() -> createDeltaExternal("delta_metric_table"))
        .hasMessageContaining("already exists");
    assertThat(tablesCreated()).isEqualTo(before + 1.0);
  }

  @Test
  @SneakyThrows
  public void icebergRestCountsOnlySuccessfullyPersistedCreates() {
    WebClient icebergClient =
        WebClient.builder(serverConfig.getServerUrl() + "/api/2.1/unity-catalog/iceberg").build();
    String tablesPath =
        "/v1/catalogs/"
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
    AggregatedHttpResponse created = postJson(icebergClient, tablesPath, body);
    assertThat(created.status().code()).as(created.contentUtf8()).isEqualTo(200);
    assertThat(tablesCreated()).isEqualTo(before + 1.0);

    AggregatedHttpResponse duplicate = postJson(icebergClient, tablesPath, body);
    assertThat(duplicate.status().code()).as(duplicate.contentUtf8()).isEqualTo(409);
    assertThat(tablesCreated()).isEqualTo(before + 1.0);
  }

  @ParameterizedTest(name = "UC REST counts successfully persisted {0} creates")
  @EnumSource(
      value = TableType.class,
      names = {"VIEW", "METRIC_VIEW"})
  @SneakyThrows
  public void viewLikeRestCountsOnlySuccessfullyPersistedCreates(TableType tableType) {
    String sourceName = "metric_source_table";
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
            .name(tableType.name().toLowerCase() + "_metric_table")
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

  private static AggregatedHttpResponse postJson(WebClient client, String path, String body) {
    return client
        .execute(
            RequestHeaders.builder(HttpMethod.POST, path).contentType(MediaType.JSON).build(), body)
        .aggregate()
        .join();
  }

  private static double tablesCreated() {
    AggregatedHttpResponse response = httpGetObservability("/metrics");

    assertThat(response.status().code()).isEqualTo(200);
    assertThat(response.contentUtf8()).contains("uc_tables_created");
    assertThat(response.contentUtf8()).contains("http_server_");

    return response
        .contentUtf8()
        .lines()
        .filter(line -> line.startsWith("uc_tables_created_total"))
        .mapToDouble(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
        .findFirst()
        .orElse(0.0);
  }
}
