package io.unitycatalog.integrationtests;

import static io.unitycatalog.integrationtests.TestUtils.ABFSS_BASE_LOCATION;
import static io.unitycatalog.integrationtests.TestUtils.AUTH_TOKEN;
import static io.unitycatalog.integrationtests.TestUtils.CATALOG_NAME;
import static io.unitycatalog.integrationtests.TestUtils.SERVER_URL;
import static io.unitycatalog.integrationtests.TestUtils.envAsBoolean;
import static io.unitycatalog.integrationtests.TestUtils.envAsLong;
import static io.unitycatalog.integrationtests.TestUtils.randomName;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.spark.UCSingleCatalog;
import java.util.List;
import java.util.UUID;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.sparkproject.guava.collect.Iterators;

/**
 * Typically, cloud vendor credentials (used to access cloud storage) issued by the UC server expire
 * after one hour. This integration test launches a long-running read-write job whose duration
 * (controlled by the 'CREDENTIAL_RENEWAL_TEST_DURATION_SECONDS' environment variable) exceeds the
 * credential’s expiration time. If the job completes successfully, it verifies that the credential
 * renewal mechanism works as expected, seamlessly refreshing credentials without interrupting the
 * ongoing job.
 *
 * <p>
 *
 * <p><b>To run this tests: </b>
 *
 * <pre>
 * export CATALOG_URI=...
 * export CATALOG_AUTH_TOKEN=...
 * export CATALOG_NAME=...
 * export S3_BASE_LOCATION=...
 * SBT_OPTS="-Xmx8G -XX:+UseG1GC" \
 * ./build/sbt \
 * "integrationTests/testOnly io.unitycatalog.integrationtests.SparkCredentialRenewalTest"
 * </pre>
 */
public class SparkCredentialRenewalTest {
  private static final String PREFIX = "CREDENTIAL_RENEWAL_TEST_";

  // Define the CREDENTIAL_RENEWAL_TEST_RENEWAL_ENABLED environment variable.
  private static final boolean RENEW_CRED_ENABLED = envAsBoolean(PREFIX + "RENEWAL_ENABLED", true);

  // Define the CREDENTIAL_RENEWAL_TEST_DURATION_SECONDS environment variable, which control how
  // long will the test run. 90 minutes by default.
  private static final long DURATION_SECONDS = envAsLong(PREFIX + "DURATION_SECONDS", 5600L);

  // Define the schema name and table name.
  private static final String SCHEMA_NAME = randomName();
  private static final String SRC_TABLE =
      String.format("%s.%s.src_%s", CATALOG_NAME, SCHEMA_NAME, randomName());
  private static final String DST_TABLE =
      String.format("%s.%s.dst_%s", CATALOG_NAME, SCHEMA_NAME, randomName());

  private static SparkSession spark;

  @BeforeAll
  public static void beforeAll() {
    String testCatalogKey = String.format("spark.sql.catalog.%s", CATALOG_NAME);
    spark =
        SparkSession.builder()
            .appName("test-credential-renewal-in-long-running-job")
            .master("local[1]") // Make it single-threaded explicitly.
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config(testCatalogKey, UCSingleCatalog.class.getName())
            .config(testCatalogKey + ".uri", SERVER_URL)
            .config(testCatalogKey + ".token", AUTH_TOKEN)
            .config(testCatalogKey + ".warehouse", CATALOG_NAME)
            .config(testCatalogKey + ".renewCredential.enabled", String.valueOf(RENEW_CRED_ENABLED))
            .getOrCreate();

    sql("CREATE SCHEMA %s.%s", CATALOG_NAME, SCHEMA_NAME);
    sql(
        "CREATE TABLE %s (id INT) USING delta LOCATION '%s/%s' PARTITIONED BY (partition INT)",
        SRC_TABLE, ABFSS_BASE_LOCATION, UUID.randomUUID());
    sql(
        "CREATE TABLE %s (id INT) USING delta LOCATION '%s/%s'",
        DST_TABLE, ABFSS_BASE_LOCATION, UUID.randomUUID());
  }

  @AfterAll
  public static void afterAll() {
    sql("DROP TABLE IF EXISTS %s", SRC_TABLE);
    sql("DROP TABLE IF EXISTS %s", DST_TABLE);
    sql("DROP SCHEMA IF EXISTS %s.%s", CATALOG_NAME, SCHEMA_NAME);
    spark.stop();
  }

  @Test
  public void testLongRunningJob() {
    // Every 100 seconds produce a row, because we don't want to produce massive small files.
    long rowCount = DURATION_SECONDS / 100 + (DURATION_SECONDS % 100 == 0 ? 0 : 1);

    // Generate data for the Delta source table.
    sql("INSERT INTO %s SELECT id, id FROM range(0, %s)", SRC_TABLE, rowCount);

    // Read from the Delta source table, mapping each partition to a separate task, and finally
    // write back to the destination table. With parallelism set to 1, tasks for each partition
    // execute sequentially. Every partition task will sleep 100 seconds, and the accumulated
    // elapsed time will finally trigger the renewal of credentials.

    // The spark job should be success because we've enabled the credential renewal.
    spark
        .read()
        .format("delta")
        .table(SRC_TABLE)
        .mapPartitions(
            (MapPartitionsFunction<Row, Integer>)
                input -> {
                  Thread.sleep(100 * 1000L);
                  return Iterators.transform(input, row -> row.getInt(0));
                },
            Encoders.INT())
        .withColumnRenamed("value", "id")
        .write()
        .format("delta")
        .mode("append")
        .saveAsTable(DST_TABLE);

    List<Row> results = sql("SELECT COUNT(*) FROM %s", DST_TABLE);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getLong(0)).isEqualTo(rowCount);
  }

  private static List<Row> sql(String statement, Object... args) {
    return spark.sql(String.format(statement, args)).collectAsList();
  }
}
