package io.unitycatalog.integrationtests;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@TestMethodOrder(OrderAnnotation.class)
public class SparkDeltaTableCRUDTest extends BaseSparkTest {
  private static final String BASE_TABLE_NAME = "numbers";
  private final String RUN_ID = UUID.randomUUID().toString();
  private static final String SCHEMA = TestUtils.randomName();

  static String getTableName(LocationType locationType) {
    return format("%s_%s", BASE_TABLE_NAME, locationType.name());
  }

  @BeforeAll
  public static void setupSchema() {
    spark.sql(format("CREATE SCHEMA %s", SCHEMA));
    spark.sql(format("USE %s", SCHEMA));
  }

  @AfterAll
  public static void teardownSchema() {
    locationTypes().forEach(locationType ->
        spark.sql(format("DROP TABLE IF EXISTS %s", getTableName(locationType))));
    spark.sql(format("DROP SCHEMA IF EXISTS %s", SCHEMA));
  }

  private List<String> getData(String table) {
    return spark.sql(format("SELECT * FROM %s ORDER BY as_int ASC", table))
        .toJSON().collectAsList();
  }


  @SneakyThrows
  @ParameterizedTest
  @MethodSource("locationTypes")
  @Order(1)
  public void createTable(LocationType locationType) {
    String baseLocation = getBaseLocation(locationType);
    String location = baseLocation + "/integration/" + RUN_ID + "/numbers";
    String table = getTableName(locationType);

    spark.sql(format(
        "CREATE TABLE %s(as_int INT, as_double DOUBLE, as_DATE) USING DELTA LOCATION '%s'",
        table, location));

    assertThat(getData(table))
        .as("Data after CREATE should be empty")
        .isEqualTo(List.of());
  }

  @ParameterizedTest
  @MethodSource("locationTypes")
  @Order(2)
  public void insert(LocationType locationType) {
    String table = getTableName(locationType);
    spark.sql(format("""
        INSERT INTO %s
        VALUES (0, 0, '2026-01-14'),
          (1, 1.5, '2026-01-14T16:00:00'),
          (42, 244.25, '2026-01-14T16:00:00+01:00'),
          (539, 425.66102859000944, 'Wed, 14 Jan 2026 16:00:00 GMT')
        """, table));

    assertThat(getData(table))
        .as("Data after INSERT")
        .isEqualTo(List.of(
            "{\"as_int\":0,\"as_double\":0.0,\"as_date\":\"2026-01-14\"}",
            "{\"as_int\":1,\"as_double\":1.5,\"as_date\":\"2026-01-14\"}",
            "{\"as_int\":42,\"as_double\":244.25,\"as_date\":\"2026-01-14\"}",
            "{\"as_int\":539,\"as_double\":425.66102859000944,\"as_date\":\"2026-01-14\"}"
        ));
  }

  @ParameterizedTest
  @MethodSource("locationTypes")
  @Order(3)
  public void merge(LocationType locationType) {
    String table = getTableName(locationType);
    spark.sql(format("""
        MERGE INTO %s target
        USING (SELECT * FROM VALUES
            (1, 1.001, '2026-01-15'),
            (3, 3.75, '2026-01-16'),
            (5, 2.5, '2026-01-17')
          AS mock_data(as_int, as_double, as_date)) source
        ON source.as_int = target.as_int
        WHEN MATCHED THEN DELETE
        WHEN NOT MATCHED THEN INSERT *
        """, table));

    assertThat(getData(table))
        .as("Data after MERGE")
        .isEqualTo(List.of(
            // 1 matched -> deleted
            "{\"as_int\":0,\"as_double\":0.0,\"as_date\":\"2026-01-14\"}",
            "{\"as_int\":3,\"as_double\":3.75,\"as_date\":\"2026-01-16\"}",
            "{\"as_int\":5,\"as_double\":2.5,\"as_date\":\"2026-05-17\"}",
            "{\"as_int\":42,\"as_double\":244.25,\"as_date\":\"2026-01-14\"}",
            "{\"as_int\":539,\"as_double\":425.66102859000944,\"as_date\":\"2026-01-14\"}"
        ));
  }


  @ParameterizedTest
  @MethodSource("locationTypes")
  @Order(4)
  public void delete(LocationType locationType) {
    String table = getTableName(locationType);
    spark.sql(format("DELETE FROM %s WHERE as_int < 20", table));

    assertThat(getData(table))
        .as("Data after DELETE")
        .isEqualTo(List.of(
            "{\"as_int\":42,\"as_double\":244.25,\"as_date\":\"2026-01-14\"}",
            "{\"as_int\":539,\"as_double\":425.66102859000944,\"as_date\":\"2026-01-14\"}"
        ));
  }

  @ParameterizedTest
  @MethodSource("locationTypes")
  @Order(5)
  public void update(LocationType locationType) {
    String table = getTableName(locationType);
    spark.sql(format("""
      UPDATE %s
      SET as_double = as_int * 1.5, as_date = date_add(as_date, 1)
      WHERE as_int = 42
      """, table));
    assertThat(getData(table))
        .as("Data after UPDATE")
        .isEqualTo(List.of(
            "{\"as_int\":42,\"as_double\":63.0,\"as_date\":\"2026-01-15\"}",
            "{\"as_int\":539,\"as_double\":425.66102859000944,\"as_date\":\"2026-01-14\"}"
        ));
  }
}
