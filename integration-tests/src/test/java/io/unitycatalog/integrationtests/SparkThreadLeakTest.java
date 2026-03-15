package io.unitycatalog.integrationtests;

import static io.unitycatalog.integrationtests.TestUtils.AUTH_TOKEN;
import static io.unitycatalog.integrationtests.TestUtils.CATALOG_NAME;
import static io.unitycatalog.integrationtests.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.integrationtests.TestUtils.SERVER_URL;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.spark.UCSingleCatalog;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies that {@code CredScopedFileSystem} does not leak {@code sdk-ScheduledExecutor} threads
 * when reading many files with credential vending enabled.
 *
 * <p>Mirrors the thread-leak repro test from <a
 * href="https://github.com/delta-io/delta/pull/6142">delta-io/delta#6142</a>. Without {@code
 * CredScopedFileSystem}, the naive fix of setting {@code fs.<scheme>.impl.disable.cache=true}
 * creates a fresh underlying filesystem — and therefore a fresh cloud SDK HTTP client — for every
 * file access. Each unclosed AWS SDK v2 client leaks 5 {@code sdk-ScheduledExecutor} threads,
 * quickly exhausting JVM resources (see <a
 * href="https://github.com/unitycatalog/unitycatalog/issues/1378">issue #1378</a>). {@code
 * CredScopedFileSystem} avoids this by keeping a static cache of real filesystem instances keyed by
 * credential scope, so clients are reused across calls that share the same credentials.
 *
 * <p><b>To run against AWS S3:</b>
 *
 * <pre>
 * export CATALOG_NAME=...
 * export SCHEMA_NAME=...
 * export CATALOG_URI=...
 * export CATALOG_AUTH_TOKEN=...
 * export S3_BASE_LOCATION=s3://&lt;bucket&gt;/&lt;prefix&gt;
 * SBT_OPTS="-Xmx8G -XX:+UseG1GC" \
 * ./build/sbt \
 * "integrationTests/testOnly io.unitycatalog.integrationtests.SparkThreadLeakTest"
 * </pre>
 *
 * <p><b>To run against GCS:</b>
 *
 * <pre>
 * export CATALOG_NAME=...
 * export SCHEMA_NAME=...
 * export CATALOG_URI=...
 * export CATALOG_AUTH_TOKEN=...
 * export GS_BASE_LOCATION=gs://&lt;bucket&gt;/&lt;prefix&gt;
 * SBT_OPTS="-Xmx8G -XX:+UseG1GC" \
 * ./build/sbt \
 * "integrationTests/testOnly io.unitycatalog.integrationtests.SparkThreadLeakTest"
 * </pre>
 *
 * <p><b>To run against Azure ADLS Gen2:</b>
 *
 * <pre>
 * export CATALOG_NAME=...
 * export SCHEMA_NAME=...
 * export CATALOG_URI=...
 * export CATALOG_AUTH_TOKEN=...
 * export ABFSS_BASE_LOCATION=abfss://&lt;container&gt;@&lt;account&gt;.dfs.core.windows.net/&lt;prefix&gt;
 * SBT_OPTS="-Xmx8G -XX:+UseG1GC" \
 * ./build/sbt \
 * "integrationTests/testOnly io.unitycatalog.integrationtests.SparkThreadLeakTest"
 * </pre>
 */
public class SparkThreadLeakTest {

  private static final String TABLE =
      String.format("%s.%s.thread_leak_test_%s", CATALOG_NAME, SCHEMA_NAME, UUID.randomUUID());

  private SparkSession spark;

  /** Only test against real cloud locations — FILE has no cloud SDK threads to leak. */
  static Stream<BaseSparkTest.LocationType> cloudLocationTypes() {
    return Stream.of(
            BaseSparkTest.LocationType.S3,
            BaseSparkTest.LocationType.GS,
            BaseSparkTest.LocationType.ABFSS)
        .filter(BaseSparkTest.LocationType::isEnabled);
  }

  private SparkSession createSparkSession() {
    String catalogKey = "spark.sql.catalog." + CATALOG_NAME;
    return SparkSession.builder()
        .appName("test-thread-leak")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config(catalogKey, UCSingleCatalog.class.getName())
        .config(catalogKey + "." + OptionsUtil.URI, SERVER_URL)
        .config(catalogKey + "." + OptionsUtil.TOKEN, AUTH_TOKEN)
        .config(catalogKey + "." + OptionsUtil.WAREHOUSE, CATALOG_NAME)
        .config(catalogKey + "." + OptionsUtil.RENEW_CREDENTIAL_ENABLED, "true")
        .getOrCreate();
  }

  @AfterEach
  public void afterEach() {
    if (spark != null) {
      try {
        spark.sql(String.format("DROP TABLE IF EXISTS %s", TABLE));
      } catch (Exception ignored) {
      }
      spark.stop();
      spark = null;
    }
  }

  /**
   * Creates a Delta table with 2 000 single-row parquet files (maximising credential-vending
   * calls), then runs 10 sequential reads and asserts that the number of leaked {@code
   * sdk-ScheduledExecutor} threads stays within an acceptable bound.
   */
  @ParameterizedTest
  @MethodSource("cloudLocationTypes")
  public void testNoSdkThreadLeakWithManyFiles(BaseSparkTest.LocationType locationType)
      throws Exception {
    spark = createSparkSession();

    String location =
        String.format("%s/thread_leak_test/%s", locationType.getBaseLocation(), UUID.randomUUID());
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, payload STRING) USING delta LOCATION '%s'", TABLE, location));

    // 1 row per parquet file → 2 000 files, maximising credential-vending calls per read.
    spark.sql("SET spark.sql.files.maxRecordsPerFile = 1");
    spark.sql(
        String.format(
            "INSERT INTO %s SELECT CAST(id AS INT), CONCAT('payload-', CAST(id AS STRING))"
                + " FROM range(2000)",
            TABLE));

    long before = countSdkThreads();
    System.out.printf(
        "[%s] sdk-ScheduledExecutor threads before reads: %d%n", locationType, before);

    for (int i = 0; i < 10; i++) {
      spark.sql(String.format("SELECT count(*) FROM %s", TABLE)).collectAsList();
    }

    long after = countSdkThreads();
    long leaked = after - before;
    System.out.printf("[%s] sdk-ScheduledExecutor threads after  reads: %d%n", locationType, after);
    System.out.printf(
        "[%s] Leaked sdk-ScheduledExecutor threads:       %d%n", locationType, leaked);
    if (leaked > 10) {
      System.out.printf(
          "[%s] THREAD LEAK DETECTED: %d threads — each group of 5 is one unclosed AWS SDK v2"
              + " client.%n",
          locationType, leaked);
    }

    // With CredScopedFileSystem the underlying cloud clients are reused within the same
    // credential scope, so the thread count must not grow significantly across reads.
    assertThat(leaked)
        .as("CredScopedFileSystem must not leak sdk-ScheduledExecutor threads on %s", locationType)
        .isLessThanOrEqualTo(10);

    spark.sql("SET spark.sql.files.maxRecordsPerFile = 0");
  }

  /** Counts active AWS SDK v2 scheduled-executor threads in the current JVM. */
  private long countSdkThreads() {
    Thread[] threads = new Thread[Thread.activeCount() * 2];
    int n = Thread.enumerate(threads);
    long count = 0;
    for (int i = 0; i < n; i++) {
      if (threads[i] != null && threads[i].getName().startsWith("sdk-ScheduledExecutor")) {
        count++;
      }
    }
    return count;
  }
}
