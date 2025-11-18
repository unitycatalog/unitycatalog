package io.unitycatalog.spark.auth;

import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.delta.tables.DeltaTable;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.spark.CredentialTestFileSystem;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.UCSingleCatalog;
import io.unitycatalog.spark.utils.Clock;
import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
public abstract class BaseCredRenewITTest extends BaseCRUDTest {
  private static final String CLOCK_NAME = UUID.randomUUID().toString();
  private static final String CATALOG_NAME = "CredRenewalCatalog";
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

  private SparkSession createSparkSession() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test-cloud-vendor-credential-renewal")
            .master("local[1]") // Make it single-threaded explicitly.
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop." + UCHadoopConf.UC_TEST_CLOCK_NAME, CLOCK_NAME)
            .config("spark.hadoop." + UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 0L)
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

    super.cleanUp();
  }

  @AfterAll
  public static void afterAll() {
    Clock.removeManualClock(CLOCK_NAME);
  }

  public static Clock testClock() {
    return Clock.getManualClock(CLOCK_NAME);
  }

  private String bucketRoot() {
    return String.format("%s://%s", scheme(), BUCKET_NAME);
  }

  @Test
  public void testFileSystemRenewal() throws Exception {
    String location = String.format("%s%s/fs", bucketRoot(), dataDir.getCanonicalPath());
    Path locPath = new Path(location);

    // Create the external delta table in catalog.
    sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", TABLE_NAME, location);

    // Insert 1 row into the table.
    sql("INSERT INTO %s VALUES (1)", TABLE_NAME);

    // Generate a table level hadoop configuration, with setting the delta table's all properties.
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
                  CredRenewFileSystem<?> fs =
                      (CredRenewFileSystem<?>) FileSystem.get(new URI(location), conf);

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
    public T generate(CredentialContext ignored) {
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
