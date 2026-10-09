package io.unitycatalog.spark.auth.storage;

import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mockStatic;

import io.delta.tables.DeltaTable;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.fs.CredScopedFileSystem;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.service.credential.CachingCloudCredentialVendor;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import io.unitycatalog.spark.CredentialTestFileSystem;
import io.unitycatalog.spark.UCSingleCatalog;
import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SerializableConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.sparkproject.guava.collect.ImmutableList;
import org.sparkproject.guava.collect.Iterators;

/**
 * Integration test to verify that cloud vendor credential renewal works as expected.
 *
 * <p>This test sets up the Unity Catalog server and a local Spark cluster, then runs a custom Spark
 * job to validate credential renewal.
 *
 * <p>The approach is as follows: a testing credential generator is injected into the local Unity
 * Catalog server, issuing a new credential every 30-second interval. The Spark job uses {@link
 * CredRenewFileSystem} to check that the credential matches the current time window for each
 * filesystem access. {@link CredRenewFileSystem} also tracks the number of renewals, allowing
 * verification that credential renewal occurs as expected.
 */
@ParameterizedClass(name = "server credential cache enabled: {0}")
@ValueSource(booleans = {false, true})
public abstract class BaseCredRenewITTest extends BaseCRUDTest {
  private static final String CLOCK_NAME = UUID.randomUUID().toString();
  protected static final String CATALOG_NAME = "CredRenewalCatalog";
  private static final String SCHEMA_NAME = "Default";
  private static final String TABLE_NAME = String.format("%s.%s.demo", CATALOG_NAME, SCHEMA_NAME);
  protected static final String BUCKET_NAME = "test-bucket";
  protected static final long DEFAULT_INTERVAL_MILLIS = 30_000L;

  @TempDir private File dataDir;
  private SparkSession session;
  private SdkSchemaOperations schemaOperations;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(createApiClient(config));
  }

  protected abstract String scheme();

  protected abstract Map<String, String> catalogExtraProps();

  /** Whether the server caches vended credentials; every test runs once with each value. */
  @Parameter boolean cacheEnabled;

  /**
   * Connector-side renewal lead in millis, written to the Spark Hadoop conf. 0 for the un-cached
   * baseline (renew only at expiry); when the cache is on it is raised above the server lead so the
   * connector re-asks the server while the cached credential is still fresh (the hit case).
   */
  private long connectorRenewalLeadMillis() {
    return cacheEnabled ? 10_000L : 0L;
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    if (cacheEnabled) {
      // Small server lead (< the connector lead, < credential validity) so the cache serves a
      // credential nearly to its true expiry — this opens the window the hit test observes.
      serverProperties.put("server.storage-credential-cache.renewal-lead-time", "PT1S");
    }
  }

  /**
   * With the cache enabled, the server vends through a caching vendor on the connector's manual
   * clock, so it judges freshness on the same timeline the vend generator uses to set expiry.
   */
  @Override
  protected void setUpCredentialOperations(ServerProperties serverProperties) {
    if (cacheEnabled) {
      cloudCredentialVendor =
          new CachingCloudCredentialVendor(
              new CloudCredentialVendor(serverProperties),
              serverProperties,
              TestUtils.clockOf(() -> testClock().now()));
    }
  }

  private SparkSession createSparkSession() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test-cloud-vendor-credential-renewal")
            .master("local[1]") // Make it single-threaded explicitly.
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop." + UCHadoopConfConstants.UC_TEST_CLOCK_NAME, CLOCK_NAME)
            .config(
                "spark.hadoop." + UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY,
                connectorRenewalLeadMillis())
            .config("spark.sql.shuffle.partitions", "1");

    // Set the default catalog properties.
    String testCatalogKey = String.format("spark.sql.catalog.%s", CATALOG_NAME);
    builder
        .config(testCatalogKey, UCSingleCatalog.class.getName())
        .config(testCatalogKey + ".uri", serverConfig.getServerUrl())
        .config(testCatalogKey + ".token", serverConfig.getAuthToken())
        .config(testCatalogKey + ".warehouse", CATALOG_NAME)
        .config(testCatalogKey + ".renewCredential.enabled", "true");

    // Set the customized catalog properties.
    catalogExtraProps().forEach(builder::config);

    return builder.getOrCreate();
  }

  private interface Callable {
    void call() throws Exception;
  }

  private void callQuietly(Callable call) {
    try {
      call.call();
    } catch (Exception e) {
      // ignored.
    }
  }

  @BeforeEach
  public void beforeEach() throws Exception {
    super.setUp();
    session = createSparkSession();

    // Initialize the catalog in unity catalog server.
    catalogOperations.createCatalog(
        new CreateCatalog().name(CATALOG_NAME).comment("Spark catalog"));

    // Initialize the schema in unity catalog server.
    schemaOperations = new SdkSchemaOperations(createApiClient(serverConfig));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
  }

  @AfterEach
  public void afterEach() {
    // Drop the table.
    sql("DROP TABLE IF EXISTS %s", TABLE_NAME);

    // Delete the scheme.
    callQuietly(() -> schemaOperations.deleteSchema(SCHEMA_NAME, Optional.of(true)));

    // Delete the catalog.
    callQuietly(() -> catalogOperations.deleteCatalog(CATALOG_NAME, Optional.of(true)));

    // Close the session.
    callQuietly(() -> session.close());
  }

  @AfterAll
  public static void afterAll() {
    Clock.removeManualClock(CLOCK_NAME);
  }

  public static Clock testClock() {
    return Clock.getManualClock(CLOCK_NAME);
  }

  /** Resets the server-side vend counter; call at the start of a measured section. */
  static void resetVendCount() {
    TimeBasedCredGenerator.GENERATE_COUNT.set(0);
  }

  /** Number of server-side vends (generator invocations) since the last {@link #resetVendCount}. */
  static int vendCount() {
    return TimeBasedCredGenerator.GENERATE_COUNT.get();
  }

  private String bucketRoot() {
    return String.format("%s://%s", scheme(), BUCKET_NAME);
  }

  @Test
  public void testFileSystemRenewal() throws Exception {
    String location = String.format("%s%s/fs", bucketRoot(), dataDir.getCanonicalPath());
    Path locPath = new Path(location);

    // Create the external Delta table in catalog.
    sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", TABLE_NAME, location);

    // Insert 1 row into the table.
    sql("INSERT INTO %s VALUES (1)", TABLE_NAME);

    // Generate a table level hadoop configuration, with setting the Delta table's all properties.
    SerializableConfiguration serialConf =
        new SerializableConfiguration(
            DeltaTable.forName(session, TABLE_NAME).deltaLog().newDeltaHadoopConf());

    // This Spark job consists of three main steps:
    // 1. Read RDD to spawn Spark tasks.
    // 2. Simulate filesystem access using the table-level Hadoop configuration to verify that
    //    filesystem credentials are renewed the expected number of times.
    // 3. Collect the credential renewal count.
    //
    // The core logic resides in the mapFunction. We use the Delta table’s Hadoop configuration
    // to initialize the filesystem, simulating Delta table operations within a Spark executor.
    // This allows us to accurately track how many times credentials are renewed within a task.
    //
    // It is possible for a Spark job to create multiple independent filesystem instances,
    // which may misleadingly appear to renew credentials correctly even when they do not.
    // We adopt this simulation approach because directly accessing a Spark task’s internal
    // filesystem instance to measure credential renewals is not feasible.
    List<Row> rows =
        session
            .read()
            .format("delta")
            .table(TABLE_NAME)
            .toJavaRDD()
            .map(
                row -> {
                  Configuration conf = serialConf.value();
                  FileSystem rawFs = FileSystem.get(new URI(location), conf);
                  // When credScopedFsEnabled=true, unwrap CredScopedFileSystem to get the real
                  // delegate. Exactly one level: newFileSystem() always restores the original impl
                  // via fs.<scheme>.impl.original, so the delegate is never CredScopedFileSystem.
                  CredRenewFileSystem<?> fs =
                      (CredRenewFileSystem<?>)
                          (rawFs instanceof CredScopedFileSystem
                              ? ((CredScopedFileSystem) rawFs).getRawFileSystem()
                              : rawFs);

                  for (int refreshIndex = 0; refreshIndex < 10; refreshIndex += 1) {
                    // Pre-check before the credential renewal.
                    fs.getFileStatus(locPath);
                    assertThat(fs.renewalCount()).isEqualTo(refreshIndex);

                    // Advance the clock to trigger the renewal.
                    testClock().sleep(Duration.ofMillis(DEFAULT_INTERVAL_MILLIS));

                    // Post-check after the credential renewal.
                    fs.getFileStatus(locPath);
                    assertThat(fs.renewalCount()).isEqualTo(refreshIndex + 1);
                  }

                  return RowFactory.create(10);
                })
            .collect();

    assertThat(rows.stream().map(r -> r.getInt(0)).collect(Collectors.toList()))
        .isEqualTo(ImmutableList.of(10));
  }

  @Test
  public void testDeltaReadWriteRenewal() throws Exception {
    String srcLoc = String.format("%s%s/src", bucketRoot(), dataDir.getCanonicalPath());
    String dstLoc = String.format("%s%s/dst", bucketRoot(), dataDir.getCanonicalPath());
    String srcTable = String.format("%s.%s.src", CATALOG_NAME, SCHEMA_NAME);
    String dstTable = String.format("%s.%s.dst", CATALOG_NAME, SCHEMA_NAME);

    // Create the source table referring to external table.
    sql(
        "CREATE TABLE %s (id INT) USING delta LOCATION '%s' PARTITIONED BY (partition INT)",
        srcTable, srcLoc);
    sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", dstTable, dstLoc);

    // Insert 1 row into each partition where id equals the partition key.
    sql("INSERT INTO %s VALUES (1, 1), (2, 2), (3, 3)", srcTable);

    // Read from the Delta table, mapping each partition to a separate task.
    // With parallelism set to 1, tasks for each partition execute sequentially.
    // The accumulated 30-second delay advances the clock sufficiently to trigger a
    // renewal of filesystem credentials.

    // The spark job should be success because we've enabled the credential renewal.
    session
        .read()
        .format("delta")
        .table(srcTable)
        .mapPartitions(
            (MapPartitionsFunction<Row, Integer>)
                input -> {
                  // Advance the clock to trigger the credential renewal.
                  testClock().sleep(Duration.ofMillis(DEFAULT_INTERVAL_MILLIS));
                  return Iterators.transform(input, row -> row.getInt(0));
                },
            Encoders.INT())
        .withColumnRenamed("value", "id")
        .write()
        .format("delta")
        .mode("append")
        .saveAsTable(dstTable);

    List<Row> rows = sql("SELECT * FROM %s ORDER BY id ASC", dstTable);
    assertThat(rows.size()).isEqualTo(3);
    assertThat(rows.stream().map(r -> r.getInt(0)).collect(Collectors.toList()))
        .isEqualTo(ImmutableList.of(1, 2, 3));
  }

  /**
   * A repeat request within a credential's validity is served from the server cache. Credentials
   * last one 30s window; the connector renews 10s before expiry and the server 1s before, so in
   * between the connector asks UC again and gets the cached credential back.
   *
   * <pre>
   *   t=0    fetch #1, vend #1   miss: the server vends
   *   t=21s  fetch #2, vend #1   connector renews; server returns its cached credential
   *   t=31s  fetch #3, vend #2   the credential expired, so the server vends a new one
   * </pre>
   */
  @Test
  public void testServesCachedCredentialWithinValidity() throws Exception {
    Assumptions.assumeTrue(cacheEnabled, "server credential cache disabled");
    String location = String.format("%s%s/hit", bucketRoot(), dataDir.getCanonicalPath());
    sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", TABLE_NAME, location);
    CredRenewFileSystem<?> fs = tableFileSystem(location);

    try (FetchRecorder fetches = new FetchRecorder(fs)) {
      advanceToNextCredentialWindow();
      resetVendCount();

      fs.getFileStatus(new Path(location));
      assertFetchesAndVends(fetches, 1, 1);

      // The connector renews and asks UC again (fetch #2), but the vend count stays at 1: the
      // server answered from its cache. The vend count is the proof; the equality only confirms
      // the connector got the window's credential back, which a re-vend would also produce.
      testClock().sleep(Duration.ofSeconds(21));
      fs.getFileStatus(new Path(location));
      assertFetchesAndVends(fetches, 2, 1);
      assertThat(fetches.fetch(1)).isEqualTo(fetches.fetch(0));

      testClock().sleep(Duration.ofSeconds(10));
      fs.getFileStatus(new Path(location));
      assertFetchesAndVends(fetches, 3, 2);
      assertThat(fetches.fetch(2)).isNotEqualTo(fetches.fetch(0));
    }
  }

  /** The table's credential-renewing filesystem, as the connector builds it on this thread. */
  private CredRenewFileSystem<?> tableFileSystem(String location) throws Exception {
    Configuration conf = DeltaTable.forName(session, TABLE_NAME).deltaLog().newDeltaHadoopConf();
    FileSystem fs = FileSystem.get(new URI(location), conf);
    return (CredRenewFileSystem<?>)
        (fs instanceof CredScopedFileSystem ? ((CredScopedFileSystem) fs).getRawFileSystem() : fs);
  }

  /** Moves the manual clock to the start of the next credential window. */
  private static void advanceToNextCredentialWindow() throws InterruptedException {
    long now = testClock().now().toEpochMilli();
    testClock().sleep(Duration.ofMillis(DEFAULT_INTERVAL_MILLIS - now % DEFAULT_INTERVAL_MILLIS));
  }

  /**
   * Records each UC credential fetch a {@link CredRenewFileSystem} makes, so a test can tell the
   * connector asking UC again apart from the server vending a new credential. Interception is
   * thread-local: only fetches on the thread that created the recorder are seen.
   */
  private static final class FetchRecorder implements AutoCloseable {
    private final CredRenewFileSystem<?> fs;
    private final MockedStatic<GenericCredentialFetcher> fetchers;
    private final List<List<GenericCredential>> fetches = new CopyOnWriteArrayList<>();

    FetchRecorder(CredRenewFileSystem<?> fs) {
      this.fs = fs;
      Configuration conf = fs.getConf();
      GenericCredentialFetcher realFetcher = GenericCredentialFetcher.create(conf);
      this.fetchers = mockStatic(GenericCredentialFetcher.class);
      fetchers
          .when(() -> GenericCredentialFetcher.create(conf))
          .thenReturn(
              (GenericCredentialFetcher)
                  () -> {
                    List<GenericCredential> credentials = realFetcher.createCredentials();
                    fetches.add(credentials);
                    return credentials;
                  });
      // A reused filesystem may already hold a provider built with an unrecorded fetcher.
      fs.lazyProvider = null;
    }

    int count() {
      return fetches.size();
    }

    /** The credentials returned by the {@code index}-th fetch, counting from zero. */
    List<GenericCredential> fetch(int index) {
      return fetches.get(index);
    }

    @Override
    public void close() {
      fetchers.close();
      fs.lazyProvider = null;
    }
  }

  private static void assertFetchesAndVends(
      FetchRecorder fetches, int expectedFetches, int expectedVends) {
    assertThat(fetches.count()).as("UC credential fetches").isEqualTo(expectedFetches);
    assertThat(vendCount()).as("server vends").isEqualTo(expectedVends);
  }

  private List<Row> sql(String statement, Object... args) {
    return session.sql(String.format(statement, args)).collectAsList();
  }

  /**
   * A customized credential provider that generates credentials based on time intervals. The entire
   * timeline is divided into consecutive 30-second windows, and all requests that fall within the
   * same window will receive the same credential. This generator is dynamically loaded by the Unity
   * Catalog server and serves credential generation requests from client REST API calls.
   */
  public abstract static class TimeBasedCredGenerator<T> {
    // Counts generate() calls; a server-cache hit does not increment it.
    static final AtomicInteger GENERATE_COUNT = new AtomicInteger();

    public T generate(CredentialContext ignored) {
      GENERATE_COUNT.incrementAndGet();
      long curTsMillis = testClock().now().toEpochMilli();
      // Align it into the window [starTs, starTs + DEFAULT_INTERVAL_MILLIS].
      long startTsMillis = curTsMillis / DEFAULT_INTERVAL_MILLIS * DEFAULT_INTERVAL_MILLIS;
      return newTimeBasedCred(startTsMillis);
    }

    protected abstract T newTimeBasedCred(long ts);
  }

  /**
   * A testing filesystem used to verify credential renewal behavior. For each {@code
   * checkCredentials()} call, the previous credentials should automatically renew as the 30-second
   * time window advances. The test tracks how many distinct credentials this filesystem receives,
   * which should match the expected number of credential renewals. We use this filesystem to
   * accurately track how many renewal happened.
   */
  public abstract static class CredRenewFileSystem<T> extends CredentialTestFileSystem {
    private final Set<Long> verifiedTs = new HashSet<>();
    private volatile T lazyProvider;

    @Override
    protected void checkCredentials(Path f) {
      String host = f.toUri().getHost();

      if (credentialCheckEnabled && BUCKET_NAME.equals(host)) {
        T provider = accessProvider();
        assertThat(provider).isNotNull();

        long curTs = testClock().now().toEpochMilli();
        long windowStartTs = curTs / DEFAULT_INTERVAL_MILLIS * DEFAULT_INTERVAL_MILLIS;
        assertCredentials(provider, windowStartTs);

        verifiedTs.add(windowStartTs);
      }
    }

    private synchronized T accessProvider() {
      if (lazyProvider == null) {
        lazyProvider = createProvider();
      }
      return lazyProvider;
    }

    protected abstract T createProvider();

    protected abstract void assertCredentials(T provider, long ts);

    public int renewalCount() {
      return verifiedTs.size() - 1;
    }
  }
}
