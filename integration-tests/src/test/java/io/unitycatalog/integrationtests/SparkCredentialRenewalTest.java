package io.unitycatalog.integrationtests;

import static io.unitycatalog.integrationtests.EnvUtils.CATALOG_NAME;
import static io.unitycatalog.integrationtests.EnvUtils.SCHEMA_NAME;
import static io.unitycatalog.integrationtests.EnvUtils.envLong;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.spark.UCSingleCatalog;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SparkCredentialRenewalTest {
  private static final long ROW_COUNT = envLong("CREDENTIAL_RENEWAL_TEST_ROW_COUNT", 10000000);
  private static final String TABLE_NAME = EnvUtils.uniqueName();

  private static SparkSession spark;

  @BeforeAll
  public static void beforeAll() {
    String testCatalogKey = String.format("spark.sql.catalog.%s", CATALOG_NAME);
    spark = SparkSession.builder()
        .appName("test-credential-renewal-in-long-running-job")
        .master("local[1]") // Make it single-threaded explicitly.
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .config(testCatalogKey, UCSingleCatalog.class.getName())
        .config(testCatalogKey + ".uri", EnvUtils.SERVER_URL)
        .config(testCatalogKey + ".token", EnvUtils.AUTH_TOKEN)
        .config(testCatalogKey + ".warehouse", CATALOG_NAME)
        .config(testCatalogKey + ".renewCredential.enabled", "true")
        .getOrCreate();

    sql("CREATE SCHEMA IF NOT EXISTS %s", SCHEMA_NAME);
    sql("CREATE TABLE %s (id BIGINT, val STRING) USING delta LOCATION '%s/%s'",
        TABLE_NAME, EnvUtils.S3_BASE_LOCATION, UUID.randomUUID());
  }

  @AfterAll
  public static void afterAll() {
    sql("DROP TABLE IF EXISTS %s", TABLE_NAME);
    sql("DROP SCHEMA IF EXISTS %s", SCHEMA_NAME);
    spark.stop();
  }

  @Test
  public void test() {
    sql("INSERT INTO %s SELECT id, CONCAT('val_', id) AS val FROM range(0, %s)",
        TABLE_NAME, ROW_COUNT);

    List<Row> results = sql("SELECT COUNT(*) FROM %s", TABLE_NAME);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getLong(0)).isEqualTo(ROW_COUNT);
  }

  private static List<Row> sql(String statement, Object... args) {
    return spark.sql(String.format(statement, args)).collectAsList();
  }
}
