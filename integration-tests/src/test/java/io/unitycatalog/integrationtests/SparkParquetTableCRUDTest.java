package io.unitycatalog.integrationtests;

import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.UUID;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestMethodOrder(OrderAnnotation.class)
public class SparkParquetTableCRUDTest extends BaseSparkTest {
    private static final String BASE_TABLE_NAME = "numbers";
    private final String RUN_ID = UUID.randomUUID().toString();
    private static final String SCHEMA = RandomStringUtils.randomAlphabetic(8);

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
        return spark.sql(format("SELECT * FROM %s ORDER BY as_int ASC", table)).toJSON().collectAsList();
    }


    @SneakyThrows
    @ParameterizedTest
    @MethodSource("locationTypes")
    @Order(1)
    public void createTable(LocationType locationType) {
        String baseLocation = getBaseLocation(locationType);
        String location = baseLocation + "/integration/" + RUN_ID + "/numbers";
        String table = getTableName(locationType);

        spark.sql(format("CREATE TABLE %s(as_int INT, as_double DOUBLE) USING PARQUET LOCATION '%s'", table, location));

        assertThat(getData(table))
                .as("Data after CREATE should be empty")
                .isEqualTo(List.of());
    }

    @ParameterizedTest
    @MethodSource("locationTypes")
    @Order(2)
    public void insert(LocationType locationType) {
        String table = getTableName(locationType);
        spark.sql(format("INSERT INTO %s VALUES (0, 0), (1, 1.5), (42, 244.25), (539, 425.66102859000944)", table));

        assertThat(getData(table))
                .as("Data after INSERT")
                .isEqualTo(List.of(
                        "{\"as_int\":0,\"as_double\":0.0}",
                        "{\"as_int\":1,\"as_double\":1.5}",
                        "{\"as_int\":42,\"as_double\":244.25}",
                        "{\"as_int\":539,\"as_double\":425.66102859000944}"
                ));
    }

    @ParameterizedTest
    @MethodSource("locationTypes")
    @Order(3)
    public void merge(LocationType locationType) {
        String table = getTableName(locationType);
        assertThatThrownBy(() -> spark.sql(format("""
                MERGE INTO %s target
                USING (SELECT * FROM VALUES (1, 1.001), (3, 3.75), (5, 2.5) AS mock_data(as_int, as_double)) source
                ON source.as_int = target.as_int
                WHEN MATCHED THEN DELETE
                WHEN NOT MATCHED THEN INSERT *
                """, table)))
                .isInstanceOf(SparkUnsupportedOperationException.class)
                .hasMessageContaining("MERGE INTO TABLE is not supported temporarily");
    }


    @ParameterizedTest
    @MethodSource("locationTypes")
    @Order(4)
    public void delete(LocationType locationType) {
        String table = getTableName(locationType);
        assertThatThrownBy(() -> spark.sql(format("DELETE FROM %s WHERE as_int < 20", table)))
                .isInstanceOf(AnalysisException.class)
                .hasMessageContaining("does not support DELETE");
    }

    @ParameterizedTest
    @MethodSource("locationTypes")
    @Order(5)
    public void update(LocationType locationType) {
        String table = getTableName(locationType);

        assertThatThrownBy(() -> spark.sql(format("UPDATE %s SET as_double = as_int * 1.5 WHERE as_int = 42", table)))
                .isInstanceOf(SparkUnsupportedOperationException.class)
                .hasMessageContaining("UPDATE TABLE is not supported temporarily");
    }
}
