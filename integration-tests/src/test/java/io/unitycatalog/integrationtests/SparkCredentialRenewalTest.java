package io.unitycatalog.integrationtests;

import static io.unitycatalog.integrationtests.TestUtils.AUTH_TOKEN;
import static io.unitycatalog.integrationtests.TestUtils.BASE_LOCATIONS;
import static io.unitycatalog.integrationtests.TestUtils.CATALOG_NAME;
import static io.unitycatalog.integrationtests.TestUtils.EXTERNAL_TABLE_TYPE;
import static io.unitycatalog.integrationtests.TestUtils.MANAGED_TABLE_TYPE;
import static io.unitycatalog.integrationtests.TestUtils.OAUTH_CLIENT_ID;
import static io.unitycatalog.integrationtests.TestUtils.OAUTH_CLIENT_SECRET;
import static io.unitycatalog.integrationtests.TestUtils.OAUTH_URI;
import static io.unitycatalog.integrationtests.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.integrationtests.TestUtils.SERVER_URL;
import static io.unitycatalog.integrationtests.TestUtils.TABLE_TYPES;
import static io.unitycatalog.integrationtests.TestUtils.envAsBoolean;
import static io.unitycatalog.integrationtests.TestUtils.envAsLong;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.spark.UCSingleCatalog;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
 * export CATALOG_NAME=...
 * export SCHEMA_NAME=...
 * export CATALOG_URI=...
 * export CATALOG_AUTH_TOKEN=...
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
  private static final long DURATION_SECONDS = envAsLong(PREFIX + "DURATION_SECONDS", 5400L);

  // Define managed table name.
  private static final String MANAGED_SRC_TABLE =
      String.format("%s.%s.managedSrcCredRenewal", CATALOG_NAME, SCHEMA_NAME);
  private static final String MANAGED_DST_TABLE =
      String.format("%s.%s.managedDstCredRenewal", CATALOG_NAME, SCHEMA_NAME);

  // Define the external table name.
  private static final String EXTERNAL_SRC_TABLE =
      String.format("%s.%s.externalSrcCredRenewal", CATALOG_NAME, SCHEMA_NAME);
  private static final String EXTERNAL_DST_TABLE =
      String.format("%s.%s.externalDstCredRenewal", CATALOG_NAME, SCHEMA_NAME);

  private static SparkSession spark;

  private static String srcTable(String tableType) {
    return MANAGED_TABLE_TYPE.equals(tableType) ? MANAGED_SRC_TABLE : EXTERNAL_SRC_TABLE;
  }

  private static String dstTable(String tableType) {
    return MANAGED_TABLE_TYPE.equals(tableType) ? MANAGED_DST_TABLE : EXTERNAL_DST_TABLE;
  }

  private void setupSparkAndTable(
      String tableType, String baseLocation, Map<String, String> catalogProps) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test-credential-renewal-in-long-running-job")
            .master("local[1]") // Make it single-threaded explicitly.
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            // Explicitly disable the Spark AQE to avoid coalescing small partitions by default.
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false");

    String testCatalogKey = String.format("spark.sql.catalog.%s", CATALOG_NAME);
    // Use the UCSingleCatalog by default.
    builder.config(testCatalogKey, UCSingleCatalog.class.getName());
    // Set all the specified catalog props.
    catalogProps.forEach((k, v) -> builder.config(testCatalogKey + "." + k, v));
    spark = builder.getOrCreate();

    sql("CREATE SCHEMA IF NOT EXISTS %s.%s", CATALOG_NAME, SCHEMA_NAME);
    if (MANAGED_TABLE_TYPE.equals(tableType)) {
      // TODO: SQL-based managed table creation is not supported yet.
      // Until that lands, the tables must be created manually in the external Unity Catalog.
      //
      // Example:
      //
      // CREATE TABLE main.demo.managedSrcCredRenewal (id INT)
      //   USING delta
      //   PARTITIONED BY (partition INT)
      //   TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported');
      //
      // CREATE TABLE main.demo.managedDstCredRenewal (id INT)
      //   USING delta
      //   TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported');
      sql("DELETE FROM %s", srcTable(tableType));
      sql("DELETE FROM %s", dstTable(tableType));
    } else {
      sql("DROP TABLE IF EXISTS %s", srcTable(tableType));
      sql("DROP TABLE IF EXISTS %s", dstTable(tableType));
      sql(
          "CREATE TABLE %s (id INT) USING delta LOCATION '%s/%s' PARTITIONED BY (partition INT)",
          srcTable(tableType), baseLocation, UUID.randomUUID());
      sql(
          "CREATE TABLE %s (id INT) USING delta LOCATION '%s/%s'",
          dstTable(tableType), baseLocation, UUID.randomUUID());
    }
  }

  @AfterEach
  public void afterEach() {
    sql("DROP TABLE IF EXISTS %s", srcTable(EXTERNAL_TABLE_TYPE));
    sql("DROP TABLE IF EXISTS %s", dstTable(EXTERNAL_TABLE_TYPE));

    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @ParameterizedTest
  @MethodSource("credentialRenewalArguments")
  public void testLongRunningJob(
      String tableType, String baseLocation, Map<String, String> catalogProps) {
    // Prepare the spark setup, and both source table and destination table creation.
    setupSparkAndTable(tableType, baseLocation, catalogProps);
    String srcTable = srcTable(tableType);
    String dstTable = dstTable(tableType);

    // Every 100 seconds produce a row, because we don't want to produce massive small files.
    long rowCount = DURATION_SECONDS / 100 + (DURATION_SECONDS % 100 == 0 ? 0 : 1);

    // Generate data for the Delta source table.
    sql("INSERT INTO %s SELECT id, id FROM range(0, %s)", srcTable, rowCount);

    // Read from the Delta source table, mapping each partition to a separate task, and finally
    // write back to the destination table. With parallelism set to 1, tasks for each partition
    // execute sequentially. Every partition task will sleep 100 seconds, and the accumulated
    // elapsed time will finally trigger the renewal of credentials.

    // The spark job should be success because we've enabled the credential renewal.
    spark
        .read()
        .format("delta")
        .table(srcTable)
        .repartition((int) rowCount)
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
        .saveAsTable(dstTable);

    List<Row> results = sql("SELECT COUNT(*) FROM %s", dstTable);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getLong(0)).isEqualTo(rowCount);
  }

  private static List<Row> sql(String statement, Object... args) {
    return spark.sql(String.format(statement, args)).collectAsList();
  }

  /**
   * Generates argument combinations for long-running tests. Each {@link Arguments} instance
   * contains:
   *
   * <ul>
   *   <li><b>managedTable</b> - true indicate covering the managed table long-running test,
   *       otherwise the external table long-running test.
   *   <li><b>baseLocation</b> – The base cloud storage path used during testing. External tables
   *       created in the tests will be placed under this location.
   *   <li><b>catalogProps</b> – A {@code Map<String, String>} of catalog configuration properties.
   *       The generated combinations cover both authentication modes: Personal Access Token (PAT)
   *       and OAuth.
   * </ul>
   *
   * @return A stream or collection of argument combinations used for parameterized tests.
   */
  public static Stream<Arguments> credentialRenewalArguments() {
    List<Arguments> testArguments = new ArrayList<>();

    for (String tableType : TABLE_TYPES) {
      for (String baseLocation : BASE_LOCATIONS) {
        if (!TestUtils.isEmptyOrNull(baseLocation)) {
          // Add the Personal Access Token (PAT) test case if specified in the env vars.
          if (!TestUtils.isEmptyOrNull(AUTH_TOKEN)) {
            testArguments.add(
                Arguments.of(
                    tableType,
                    baseLocation,
                    Map.of(
                        OptionsUtil.URI, SERVER_URL,
                        OptionsUtil.TOKEN, AUTH_TOKEN,
                        OptionsUtil.WAREHOUSE, CATALOG_NAME,
                        OptionsUtil.RENEW_CREDENTIAL_ENABLED, String.valueOf(RENEW_CRED_ENABLED))));
          }

          // Add the OAuth test case if specified in env vars.
          if (!TestUtils.isEmptyOrNull(OAUTH_URI)) {
            testArguments.add(
                Arguments.of(
                    tableType,
                    baseLocation,
                    Map.of(
                        OptionsUtil.URI, SERVER_URL,
                        OptionsUtil.OAUTH_URI, OAUTH_URI,
                        OptionsUtil.OAUTH_CLIENT_ID, OAUTH_CLIENT_ID,
                        OptionsUtil.OAUTH_CLIENT_SECRET, OAUTH_CLIENT_SECRET,
                        OptionsUtil.WAREHOUSE, CATALOG_NAME,
                        OptionsUtil.RENEW_CREDENTIAL_ENABLED, String.valueOf(RENEW_CRED_ENABLED))));
          }
        }
      }
    }

    return testArguments.stream();
  }
}
