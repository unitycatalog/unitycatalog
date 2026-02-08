package io.unitycatalog.spark;

import static io.unitycatalog.server.base.BaseServerTest.serverConfig;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableInfoViewDependencies;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.utils.JsonUtils;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import okhttp3.Headers;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ViewsTest {
  private MockWebServer mockUnityCatalogServer;
  private SparkSession spark;
  private static final String CATALOG_NAME = "test_catalog";

  @SneakyThrows(IOException.class)
  @BeforeEach
  public void setUp() {
    mockUnityCatalogServer = new MockWebServer();
    mockUnityCatalogServer.start();
    spark = sparkSession(CATALOG_NAME, mockUnityCatalogServer);
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
    mockUnityCatalogServer.close();
  }

  @SneakyThrows
  @Test
  public void testViewsReturnedByUCSingleCatalogAsTablesCanBeUsedBySpark() {
    String schemaName = "test";
    String viewName = "simple_view";
    String viewFullName = CATALOG_NAME + "." + schemaName + "." + viewName;

    // Given a view
    String viewDefinition =
        "SELECT * FROM VALUES (1, 'John', 13), (2, 'Tom', 43), (4, 'Mary', 2), (5, 'Tim', 84) AS (id, name, age)";
    TableInfo view =
        new TableInfo()
            .name(viewName)
            .catalogName(CATALOG_NAME)
            .schemaName(schemaName)
            .tableType(TableType.VIEW)
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("id")
                        .typeText("int")
                        .typeName(ColumnTypeName.INT)
                        .position(0)
                        .typePrecision(0)
                        .typeScale(0)
                        .typeJson(
                            "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}")
                        .nullable(false),
                    new ColumnInfo()
                        .name("name")
                        .typeText("string")
                        .typeName(ColumnTypeName.STRING)
                        .position(1)
                        .typePrecision(0)
                        .typeScale(0)
                        .typeJson(
                            "{\"name\":\"name\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}")
                        .nullable(false),
                    new ColumnInfo()
                        .name("age")
                        .typeText("int")
                        .typeName(ColumnTypeName.INT)
                        .position(2)
                        .typePrecision(0)
                        .typeScale(0)
                        .typeJson(
                            "{\"name\":\"age\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}")
                        .nullable(false)))
            .owner("tom@example.com")
            .createdAt(1767261705238L)
            .createdBy("tom@example.com")
            .updatedAt(1767261705238L)
            .updatedBy("tom@example.com")
            .tableId("AAAA-BBBB-CCCC-DDDD-1234")
            .viewDefinition(viewDefinition)
            .viewDependencies(new TableInfoViewDependencies());

    mockUnityCatalogServer.enqueue(
        new MockResponse(200, Headers.EMPTY, JsonUtils.getInstance().writeValueAsString(view)));

    // When we use the view (force materialization deterministically)
    List<PersonRow> rows =
        spark
            .sql("SELECT * FROM " + viewFullName + " WHERE age >= 18 ORDER BY id")
            .as(Encoders.bean(PersonRow.class))
            .collectAsList();

    // Then UCSingleCatalog should call /tables endpoint attempting to read the view (exactly once)
    RecordedRequest recordedRequest = mockUnityCatalogServer.takeRequest(5, TimeUnit.SECONDS);
    assertThat(recordedRequest).as("expected a single HTTP request to UC").isNotNull();
    assertThat(recordedRequest.getMethod()).isEqualTo("GET");
    assertThat(recordedRequest.getUrl().encodedPath())
        .isEqualTo("/api/2.1/unity-catalog/tables/" + viewFullName);

    // No additional unexpected calls
    RecordedRequest extraRequest = mockUnityCatalogServer.takeRequest(10, TimeUnit.MILLISECONDS);
    assertThat(extraRequest).as("no extra UC requests expected").isNull();

    // And the data from the view should be utilized
    assertThat(rows).containsExactly(new PersonRow(2, "Tom", 43), new PersonRow(5, "Tim", 84));
  }

  /**
   * Creates a spark session with a single Unity Catalog catalog with HTTP backend that can be
   * mocked.
   */
  private SparkSession sparkSession(String catalogName, MockWebServer unityCatalogServer) {
    String catalogConf = "spark.sql.catalog." + catalogName;
    return SparkSession.builder()
        .appName("test")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(catalogConf, UCSingleCatalog.class.getName())
        .config(
            catalogConf + "." + OptionsUtil.URI,
            "http://" + unityCatalogServer.getHostName() + ":" + unityCatalogServer.getPort())
        .config(catalogConf + "." + OptionsUtil.TOKEN, serverConfig.getAuthToken())
        .config(catalogConf + "." + OptionsUtil.WAREHOUSE, catalogName)
        .getOrCreate();
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  private static class PersonRow {
    private int id;
    private String name;
    private int age;
  }
}
