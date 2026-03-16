package io.unitycatalog.integrationtests;

import static io.unitycatalog.integrationtests.TestUtils.AUTH_TOKEN;
import static io.unitycatalog.integrationtests.TestUtils.CATALOG_NAME;
import static io.unitycatalog.integrationtests.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.integrationtests.TestUtils.SERVER_URL;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.spark.UCSingleCatalog;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.UUID;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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
 * <p>The test uses a UC managed table so no external storage location is required. The catalog must
 * be configured in UC with a managed storage location backed by cloud storage (S3, GCS, or ADLS
 * Gen2) to exercise credential vending.
 *
 * <p><b>To run:</b>
 *
 * <pre>
 * export CATALOG_NAME=...
 * export SCHEMA_NAME=...
 * export CATALOG_URI=...
 * export CATALOG_AUTH_TOKEN=...
 * SBT_OPTS="-Xmx8G -XX:+UseG1GC" \
 * ./build/sbt \
 * "integrationTests/testOnly io.unitycatalog.integrationtests.SparkThreadLeakTest"
 * </pre>
 */
public class SparkThreadLeakTest {

  private static final String TABLE =
      String.format("%s.%s.thread_leak_test_%s", CATALOG_NAME, SCHEMA_NAME, UUID.randomUUID());

  private SparkSession spark;

  private SparkSession createSparkSession() {
    String catalogKey = "spark.sql.catalog." + CATALOG_NAME;
    return SparkSession.builder()
        .appName("test-thread-leak")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(catalogKey, UCSingleCatalog.class.getName())
        .config(catalogKey + "." + OptionsUtil.URI, SERVER_URL)
        .config(catalogKey + "." + OptionsUtil.TOKEN, AUTH_TOKEN)
        .config(catalogKey + "." + OptionsUtil.WAREHOUSE, CATALOG_NAME)
        .config(catalogKey + "." + OptionsUtil.RENEW_CREDENTIAL_ENABLED, "true")
        .config(catalogKey + "." + OptionsUtil.CRED_SCOPED_FS_ENABLED, "true")
        .getOrCreate();
  }

  @AfterEach
  public void afterEach() {
    if (spark != null) {
      try {
        spark.sql(String.format("DROP TABLE IF EXISTS %s", TABLE));
      } catch (Exception ignored) {
        // best-effort cleanup; ignore failures
      }
      spark.stop();
      spark = null;
    }
  }

  /**
   * Creates a UC managed Delta table with 2 000 single-row parquet files (maximising
   * credential-vending calls), then runs 10 sequential reads and asserts that the number of leaked
   * {@code sdk-ScheduledExecutor} threads stays within an acceptable bound.
   */
  @Test
  public void testNoSdkThreadLeakWithManyFiles() {
    spark = createSparkSession();

    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, payload STRING) USING delta"
                + " TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')",
            TABLE));

    // 1 row per parquet file → 2 000 files, maximising credential-vending calls per read.
    spark.sql("SET spark.sql.files.maxRecordsPerFile = 1");
    spark.sql(
        String.format(
            "INSERT INTO %s SELECT CAST(id AS INT), CONCAT('payload-', CAST(id AS STRING))"
                + " FROM range(2000)",
            TABLE));

    long before = countSdkThreads();
    System.out.printf("sdk-ScheduledExecutor threads before reads: %d%n", before);

    for (int i = 0; i < 10; i++) {
      spark.sql(String.format("SELECT count(*) FROM %s", TABLE)).collectAsList();
    }

    long after = countSdkThreads();
    long leaked = after - before;
    System.out.printf("sdk-ScheduledExecutor threads after  reads: %d%n", after);
    System.out.printf("Leaked sdk-ScheduledExecutor threads:       %d%n", leaked);
    if (leaked > 10) {
      System.out.printf(
          "THREAD LEAK DETECTED: %d threads — each group of 5 is one unclosed AWS SDK v2"
              + " client.%n",
          leaked);
    }

    // With CredScopedFileSystem the underlying cloud clients are reused within the same
    // credential scope, so the thread count must not grow significantly across reads.
    assertThat(leaked)
        .as("CredScopedFileSystem must not leak sdk-ScheduledExecutor threads")
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
