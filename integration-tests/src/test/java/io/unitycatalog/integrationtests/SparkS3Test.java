package io.unitycatalog.integrationtests;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class SparkS3Test extends BaseSparkTest {
    // todo: table creates don't work w/ vended creds yet
    // private final String S3_TEST_DELTA_LOCATION = System.getenv("S3_TEST_DELTA_LOCATION");
    // private final String S3_TEST_PARQUET_FILE = System.getenv("S3_TEST_PARQUET_FILE");

    @Test
    public void readS3VendedCredentialsDelta() {
        // todo: table creates don't work w/ vended creds yet
        // String cmd = format("CREATE TABLE IF NOT EXISTS s3.numbers_s3(as_int INT, as_double DOUBLE) USING DELTA LOCATION '%s'", S3_TEST_DELTA_LOCATION);
        // spark.sql(cmd);

        List<String> actual = spark.sql("SELECT * FROM s3.numbers_s3").toJSON().collectAsList();
        List<String> expect = getExpectData("read_numbers.json");
        assertThat(actual).isEqualTo(expect);
    }

    @Test
    public void readS3VendedCredentialsParquet() {
        // todo: table creates don't work w/ vended creds yet
        // String cmd = format("CREATE TABLE IF NOT EXISTS s3.numbers_s3(as_int INT, as_double DOUBLE) USING DELTA LOCATION '%s'", S3_TEST_PARQUET_FILE);
        // spark.sql(cmd);

        List<String> actual = spark.sql("SELECT * FROM s3.numbers_s3_parquet").toJSON().collectAsList();
        List<String> expect = getExpectData("read_numbers.json");
        assertThat(actual).isEqualTo(expect);
    }
}
