package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.TableType;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

public class IcebergRestCatalogViewE2ETest extends BaseSparkIntegrationTest {

  private static final String ICEBERG_REST_CATALOG = "iceberg_rest";

  @Test
  @EnabledIf("hasIcebergSparkRuntime")
  public void testSparkReadsUcSqlViewThroughIcebergRestCatalog() throws ApiException {
    String viewName = "constant_view";
    createSqlView(viewName);
    session = createIcebergRestSparkSession();

    List<Row> rows =
        session
            .sql(
                String.format(
                    "SELECT as_int, as_string FROM %s.%s.%s",
                    ICEBERG_REST_CATALOG, SCHEMA_NAME, viewName))
            .collectAsList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(42);
    assertThat(rows.get(0).getString(1)).isEqualTo("ready");
  }

  private void createSqlView(String viewName) throws ApiException {
    TablesApi tablesApi = new TablesApi(createApiClient(serverConfig));
    ColumnInfo intColumn =
        new ColumnInfo()
            .name("as_int")
            .typeText("INTEGER")
            .typeJson(
                "{\"name\":\"as_int\",\"type\":\"integer\"," + "\"nullable\":true,\"metadata\":{}}")
            .typeName(ColumnTypeName.INT)
            .position(0)
            .nullable(true);
    ColumnInfo stringColumn =
        new ColumnInfo()
            .name("as_string")
            .typeText("STRING")
            .typeJson(
                "{\"name\":\"as_string\",\"type\":\"string\","
                    + "\"nullable\":true,\"metadata\":{}}")
            .typeName(ColumnTypeName.STRING)
            .position(1)
            .nullable(true);
    tablesApi.createTable(
        new CreateTable()
            .name(viewName)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .columns(List.of(intColumn, stringColumn))
            .tableType(TableType.fromValue("VIEW"))
            .viewDefinition("SELECT 42 AS as_int, 'ready' AS as_string"));
  }

  private SparkSession createIcebergRestSparkSession() {
    String catalogConf = "spark.sql.catalog." + ICEBERG_REST_CATALOG;
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("iceberg-rest-view-e2e")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(catalogConf, "org.apache.iceberg.spark.SparkCatalog")
            .config(catalogConf + ".type", "rest")
            .config(
                catalogConf + ".uri",
                serverConfig.getServerUrl() + "/api/2.1/unity-catalog/iceberg")
            .config(catalogConf + ".warehouse", CATALOG_NAME);
    if (serverConfig.getAuthToken() != null && !serverConfig.getAuthToken().isEmpty()) {
      builder.config(catalogConf + ".token", serverConfig.getAuthToken());
    }
    return builder.getOrCreate();
  }

  private boolean hasIcebergSparkRuntime() {
    try {
      Class.forName("org.apache.iceberg.spark.SparkCatalog");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
