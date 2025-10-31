package io.unitycatalog.integrationtests;

import static io.unitycatalog.integrationtests.TestUtils.AUTH_TOKEN;
import static io.unitycatalog.integrationtests.TestUtils.CATALOG_NAME;
import static io.unitycatalog.integrationtests.TestUtils.S3_BASE_LOCATION;
import static io.unitycatalog.integrationtests.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.integrationtests.TestUtils.SERVER_URL;
import static io.unitycatalog.integrationtests.TestUtils.envBoolean;
import static io.unitycatalog.integrationtests.TestUtils.envLong;
import static io.unitycatalog.integrationtests.TestUtils.randomName;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.spark.UCSingleCatalog;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Typically, cloud vendor credentials (used to access cloud storage) issued by the UC server
 * expire after one hour. This integration test launches a long-running write job whose duration
 * (controlled by the {@link SparkCredentialRenewalTest#ROW_COUNT} environment variable) exceeds
 * the credential’s expiration time. If the job completes successfully, it verifies that the
 * credential renewal mechanism works as expected, seamlessly refreshing credentials without
 * interrupting the ongoing job.
 */
public class SparkCredentialRenewalTest {
  private static final String PREFIX = "CREDENTIAL_RENEWAL_TEST_";

  // Define the CREDENTIAL_RENEWAL_TEST_RENEWAL_ENABLED environment variable.
  private static final boolean RENEW_CRED_ENABLED = envBoolean(PREFIX + "RENEWAL_ENABLED", true);

  // Define the CREDENTIAL_RENEWAL_TEST_ROW_COUNT environment variable.
  private static final long ROW_COUNT = envLong(PREFIX + "ROW_COUNT", 10_000_000_000L);

  // Define the table name.
  private static final String TABLE_NAME = String.format("%s.%s.%s",
      CATALOG_NAME, SCHEMA_NAME, randomName());

  private static SparkSession spark;

  @BeforeAll
  public static void beforeAll() {
    String testCatalogKey = String.format("spark.sql.catalog.%s", CATALOG_NAME);
    spark = SparkSession.builder()
        .appName("test-credential-renewal-in-long-running-job")
        .master("local[1]") // Make it single-threaded explicitly.
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(testCatalogKey, UCSingleCatalog.class.getName())
        .config(testCatalogKey + ".uri", SERVER_URL)
        .config(testCatalogKey + ".token", AUTH_TOKEN)
        .config(testCatalogKey + ".warehouse", CATALOG_NAME)
        .config(testCatalogKey + ".renewCredential.enabled", String.valueOf(RENEW_CRED_ENABLED))
        .getOrCreate();

    sql("CREATE SCHEMA %s", SCHEMA_NAME);
    sql("CREATE TABLE %s (id BIGINT, val STRING) USING delta LOCATION '%s/%s'",
        TABLE_NAME, S3_BASE_LOCATION, UUID.randomUUID());
  }

  @AfterAll
  public static void afterAll() {
    sql("DROP TABLE IF EXISTS %s", TABLE_NAME);
    sql("DROP SCHEMA IF EXISTS %s", SCHEMA_NAME);
    spark.stop();
  }

  @Test
  public void testLongRunningJob() {
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
