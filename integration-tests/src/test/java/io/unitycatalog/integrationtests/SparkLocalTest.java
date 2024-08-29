package io.unitycatalog.integrationtests;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.List;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class SparkLocalTest extends BaseSparkTest {
    @Test
    public void listSchemas() {
        List<String> actual = spark.sql("show schemas").toJSON().collectAsList();
        List<String> expect = getExpectData("list_schemas.json");
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void listTables() {
        List<String> actual = spark.sql("SHOW TABLES IN default").toJSON().collectAsList();
        List<String> expect = getExpectData("list_tables.json");
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void readManagedDeltaTable() {
        List<String> actual = spark.sql("SELECT * FROM default.marksheet").toJSON().collectAsList();
        List<String> expect = getExpectData("read_marksheet.json");
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void readExternalParquetTable() {
        List<String> actual = spark.sql("SELECT * FROM default.numbers_parquet").toJSON().collectAsList();
        List<String> expect = getExpectData("read_numbers.json");
        assertThat(actual).isEqualTo(expect);
    }

    @SneakyThrows
    @Test
    public void deltaCRUD() {
        // todo: break this up a bit / split into multiple tests
        String SCHEMA = "test_schema";
        String TABLE = "test_table";

        String tmpdir = Files.createTempDirectory("uc-test-table").toFile().getAbsolutePath();
        try {
            spark.sql(format("CREATE SCHEMA %s", SCHEMA));
            spark.sql(format("CREATE TABLE %s.%s (id INT) USING DELTA LOCATION '%s'", SCHEMA, TABLE, tmpdir));
            spark.sql(format("DELETE FROM %s.%s", SCHEMA, TABLE));
            spark.sql(format("INSERT INTO %s.%s VALUES (1),(2),(3)", SCHEMA, TABLE));
            List<String> actual = spark.sql(format("SELECT * FROM %s.%s ORDER BY 1", SCHEMA, TABLE)).toJSON().collectAsList();
            List<String> expect = List.of("{\"id\":1}", "{\"id\":2}", "{\"id\":3}");
            assertThat(actual).isEqualTo(expect);
        } finally {
            spark.sql(format("DROP TABLE IF EXISTS %s.%s", SCHEMA, TABLE));
            spark.sql(format("DROP SCHEMA IF EXISTS %s", SCHEMA));
        }

    }
}
