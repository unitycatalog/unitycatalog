package io.unitycatalog.spark.auth.storage;

import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.AwsCredentialGenerator;
import io.unitycatalog.spark.CredentialTestFileSystem;
import io.unitycatalog.spark.UCSingleCatalog;
import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * End-to-end concurrency test for the connector credential cache (#1651). Boots a real Unity
 * Catalog server plus a local Spark session, then verifies through the full
 * loadTable-and-vend-credentials stack that:
 *
 * <ul>
 *   <li>a slow credential vend for one table does not block queries on other tables, and
 *   <li>concurrent queries on the same table vend its credential exactly once (single flight).
 * </ul>
 *
 * <p>Slowness is injected into the server-side credential generator (same JVM), gated on a static
 * flag and the table location, so setup traffic is unaffected. The slow vend parks on a latch the
 * test releases only after the fast-table query completes, so the pass/fail signal is an ordering
 * fact rather than a timing margin; a bounded fallback await keeps the server worker from being
 * parked indefinitely.
 */
public class AwsCredCacheConcurrencyITTest extends BaseCRUDTest {

  private static final String CATALOG_NAME = "CredConcurrencyCatalog";
  private static final String SCHEMA_NAME = "Default";
  private static final String BUCKET_NAME = "test-bucket";
  private static final String SLOW_TABLE_DIR = "slowtbl";
  private static final String FAST_TABLE_DIR = "fasttbl";
  private static final String WARM_TABLE_DIR = "warmtbl";
  private static final long SLOW_VEND_FALLBACK_MILLIS = 60_000L;
  private static final long CRED_WINDOW_MILLIS = 30_000L;
  private static final String CLOCK_NAME = UUID.randomUUID().toString();

  private static final String SLOW_TABLE = table("slow_tbl");
  private static final String FAST_TABLE = table("fast_tbl");
  private static final String WARM_TABLE = table("warm_tbl");

  private static volatile boolean slowVendEnabled = false;
  private static volatile CountDownLatch slowVendStarted = new CountDownLatch(1);
  private static volatile CountDownLatch releaseSlowVend = new CountDownLatch(1);
  private static final AtomicInteger slowVends = new AtomicInteger();
  private static final AtomicInteger fastVends = new AtomicInteger();

  @TempDir private File dataDir;
  private SparkSession session;
  private SdkSchemaOperations schemaOperations;

  private static String table(String name) {
    return String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, name);
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(createApiClient(config));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty("s3.bucketPath.0", "s3://" + BUCKET_NAME);
    serverProperties.setProperty("s3.accessKey.0", "accessKey0");
    serverProperties.setProperty("s3.secretKey.0", "secretKey0");
    serverProperties.setProperty("s3.sessionToken.0", "sessionToken0");
    serverProperties.setProperty(
        "s3.credentialGenerator.0", SlowScopeAwsCredGenerator.class.getName());
  }

  // BaseCRUDTest.setUp() is itself @BeforeEach and has already run (superclass lifecycle methods
  // execute first), so this method must not call super.setUp() again -- doing so would boot and
  // leak a second server.
  @BeforeEach
  public void beforeEach() throws Exception {
    slowVendEnabled = false;
    slowVendStarted = new CountDownLatch(1);
    releaseSlowVend = new CountDownLatch(1);
    slowVends.set(0);
    fastVends.set(0);

    String catalogKey = String.format("spark.sql.catalog.%s", CATALOG_NAME);
    session =
        SparkSession.builder()
            .appName("test-credential-cache-concurrency")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "1")
            .config(catalogKey, UCSingleCatalog.class.getName())
            .config(catalogKey + ".uri", serverConfig.getServerUrl())
            .config(catalogKey + ".token", serverConfig.getAuthToken())
            .config(catalogKey + ".warehouse", CATALOG_NAME)
            .config("spark.hadoop." + UCHadoopConfConstants.UC_TEST_CLOCK_NAME, CLOCK_NAME)
            .config("spark.hadoop." + UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 0L)
            .config("fs.s3.impl", PlainS3FileSystem.class.getName())
            .getOrCreate();

    catalogOperations.createCatalog(
        new CreateCatalog().name(CATALOG_NAME).comment("Spark catalog"));
    schemaOperations = new SdkSchemaOperations(createApiClient(serverConfig));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
  }

  @AfterEach
  public void afterEach() {
    slowVendEnabled = false;
    for (String tableName : List.of(SLOW_TABLE, FAST_TABLE, WARM_TABLE)) {
      try {
        sql("DROP TABLE IF EXISTS %s", tableName);
      } catch (Exception e) {
        // Ignored.
      }
    }
    try {
      schemaOperations.deleteSchema(SCHEMA_NAME, Optional.of(true));
      catalogOperations.deleteCatalog(CATALOG_NAME, Optional.of(true));
    } catch (Exception e) {
      // Ignored.
    }
    try {
      if (session != null) {
        session.close();
      }
    } catch (Exception e) {
      // Ignored.
    }
  }

  @Test
  public void slowScopeVendDoesNotBlockOtherScopes() throws Exception {
    String root = String.format("s3://%s%s", BUCKET_NAME, dataDir.getCanonicalPath());
    sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s/%s'", SLOW_TABLE, root, SLOW_TABLE_DIR);
    sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s/%s'", FAST_TABLE, root, FAST_TABLE_DIR);
    sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s/%s'", WARM_TABLE, root, WARM_TABLE_DIR);
    sql("INSERT INTO %s VALUES (1)", SLOW_TABLE);
    sql("INSERT INTO %s VALUES (2)", FAST_TABLE);
    sql("INSERT INTO %s VALUES (3)", WARM_TABLE);
    // Warm the Spark SQL read path (codegen etc.) so the timed fast query below measures
    // credential vending plus a routine local read, not first-query initialization.
    sql("SELECT * FROM %s", WARM_TABLE);

    // Advance the shared manual clock past the credential window so every credential vended
    // during setup is stale and the next access per table must re-vend.
    slowVendEnabled = true;
    testClock().sleep(Duration.ofMillis(CRED_WINDOW_MILLIS));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      // First read of the slow table vends its READ credential; the generator holds that vend
      // for SLOW_VEND_MILLIS.
      Future<List<Row>> slowQuery = executor.submit(() -> sql("SELECT * FROM %s", SLOW_TABLE));
      assertThat(slowVendStarted.await(60, TimeUnit.SECONDS)).isTrue();

      // While the slow vend is parked, a first read of a different table must vend its own
      // credential and complete. The slow vend is only released AFTER this query finishes, so
      // on a coarse-locked cache this query cannot complete and the test fails by ordering,
      // not by timing.
      List<Row> fastRows = sql("SELECT * FROM %s", FAST_TABLE);
      assertThat(fastRows.size()).isEqualTo(1);
      assertThat(fastVends.get()).isEqualTo(1);
      releaseSlowVend.countDown();

      // A concurrent second read of the slow table must reuse the vended credential.
      Future<List<Row>> secondSlowQuery =
          executor.submit(() -> sql("SELECT * FROM %s", SLOW_TABLE));

      assertThat(slowQuery.get(60, TimeUnit.SECONDS).size()).isEqualTo(1);
      assertThat(secondSlowQuery.get(60, TimeUnit.SECONDS).size()).isEqualTo(1);
      assertThat(slowVends.get())
          .as("concurrent queries on the slow table must vend its credential exactly once")
          .isEqualTo(1);
    } finally {
      slowVendEnabled = false;
      releaseSlowVend.countDown();
      executor.shutdownNow();
    }
  }

  private List<Row> sql(String statement, Object... args) {
    return session.sql(String.format(statement, args)).collectAsList();
  }

  private static Clock testClock() {
    return Clock.getManualClock(CLOCK_NAME);
  }

  @AfterAll
  public static void afterAll() {
    Clock.removeManualClock(CLOCK_NAME);
  }

  /**
   * Issues long-lived static credentials; when enabled, holds the vend for the slow table's
   * location for a bounded interval and counts vends per scope.
   */
  public static class SlowScopeAwsCredGenerator implements AwsCredentialGenerator {
    @Override
    public Credentials generate(CredentialContext ctx) {
      String locations = String.valueOf(ctx.getLocations());
      if (slowVendEnabled && locations.contains(SLOW_TABLE_DIR)) {
        slowVends.incrementAndGet();
        slowVendStarted.countDown();
        try {
          // Parked until the test observes the fast-table query completing; the bounded await
          // guarantees a server worker is never held indefinitely if the test dies first.
          releaseSlowVend.await(SLOW_VEND_FALLBACK_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      } else if (slowVendEnabled && locations.contains(FAST_TABLE_DIR)) {
        fastVends.incrementAndGet();
      }
      // Window-aligned expiry on the shared manual clock: with renewal lead time 0 on the
      // connector, credentials go stale exactly when the test advances the clock a full window.
      long windowStart = testClock().now().toEpochMilli() / CRED_WINDOW_MILLIS * CRED_WINDOW_MILLIS;
      return Credentials.builder()
          .accessKeyId("accessKeyId")
          .secretAccessKey("secretAccessKey")
          .sessionToken("sessionToken")
          .expiration(Instant.ofEpochMilli(windowStart + CRED_WINDOW_MILLIS))
          .build();
    }
  }

  /** Maps {@code s3://test-bucket/...} to the local filesystem; no credential assertions. */
  public static class PlainS3FileSystem extends CredentialTestFileSystem {
    @Override
    protected String scheme() {
      return "s3:";
    }

    @Override
    protected void checkCredentials(Path f) {
      // No-op: this test verifies vending concurrency, not credential contents.
    }
  }
}
