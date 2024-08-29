package io.unitycatalog.integrationtests;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

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
}
