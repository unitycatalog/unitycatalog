package io.unitycatalog.spark.auth;

import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.auth.oauth2.AccessToken;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import io.delta.tables.DeltaTable;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.spark.CredentialTestFileSystem;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.UCSingleCatalog;
import io.unitycatalog.spark.utils.Clock;
import java.io.File;
import java.net.URI;
import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
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

public class GcsCredentialRenewalTest extends BaseCRUDTest {

  // Name of the manual clock used to drive the credential renewal specific to GCS Credential
  // Renewal integration tests.
  // Clocks need a new name for each test class to avoid conflicts.
  private static final String CLOCK_NAME = GcsCredentialRenewalTest.class.getSimpleName();
  private static final String CATALOG_NAME = "CredRenewalCatalog";
  private static final String SCHEMA_NAME = "Default";
  private static final String TABLE_NAME = String.format("%s.%s.demo", CATALOG_NAME, SCHEMA_NAME);
  private static final String BUCKET_NAME = "test-bucket";
  private static final String GCS_SCHEME = "gs:";
  // Default interval for credential renewal is 30 seconds. (issue a new credential every 30 seconds
  // for tests)
  private static final long DEFAULT_INTERVAL_MILLIS = 30_000L;

  @TempDir private File dataDir;
  private SparkSession session;
  private SdkSchemaOperations schemaOperations;

  /**
   * Create an SDK client aimed at the embedded server so catalog CRUD uses the full REST stack.
   * This keeps the integration realistic, with HTTP serialization and auth headers applied.
   */
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(createApiClient(serverConfig));
  }

  /**
   * Configure the server to use the test-specific credential generator that issues time-bucketed
   * tokens aligned with the manual clock. This matches the AWS renewal test approach.
   */
  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.put("gcs.bucketPath.0", "gs://" + BUCKET_NAME);
    serverProperties.put("gcs.jsonKeyFilePath.0", "");
    serverProperties.put(
        "gcs.credentialsGenerator.0", TestingRenewalCredentialsGenerator.class.getName());
  }

  /**
   * Build a Spark session that shares the manual clock with the server. Renewal lead time is
   * disabled so the test can control exactly when refreshes occur. Hadoop config registers the
   * GCS-specific filesystem wrapper for token introspection.
   */
  private SparkSession createSparkSession() {
    String testCatalogKey = String.format("spark.sql.catalog.%s", CATALOG_NAME);
    return SparkSession.builder()
        .appName("test-gcs-credential-renewal")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop." + UCHadoopConf.UC_TEST_CLOCK_NAME, CLOCK_NAME)
        .config("spark.hadoop." + UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 0L)
        .config("spark.sql.shuffle.partitions", "1")
        .config(testCatalogKey, UCSingleCatalog.class.getName())
        .config(testCatalogKey + ".uri", serverConfig.getServerUrl())
        .config(testCatalogKey + ".token", serverConfig.getAuthToken())
        .config(testCatalogKey + ".warehouse", CATALOG_NAME)
        .config(testCatalogKey + ".renewCredential.enabled", "true")
        .config("fs.gs.impl", GcsCredFileSystem.class.getName())
        .config("fs.gs.impl.disable.cache", "true")
        .getOrCreate();
  }

  private interface Callable {
    void call() throws Exception;
  }

  private void callQuietly(Callable call) {
    try {
      call.call();
    } catch (Exception e) {
      // ignore for cleanup paths
    }
  }

  @BeforeEach
  public void beforeEach() throws Exception {
    super.setUp();
    session = createSparkSession();

    catalogOperations.createCatalog(
        new CreateCatalog().name(CATALOG_NAME).comment("Spark catalog"));

    schemaOperations = new SdkSchemaOperations(createApiClient(serverConfig));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
  }

  @AfterEach
  public void afterEach() {
    sql("DROP TABLE IF EXISTS %s", TABLE_NAME);

    callQuietly(() -> schemaOperations.deleteSchema(SCHEMA_NAME, Optional.of(true)));
    callQuietly(() -> catalogOperations.deleteCatalog(CATALOG_NAME, Optional.of(true)));
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

  /**
   * Parallels AwsCredentialRenewalTest#testFileSystemRenewal, but exercises the GCS provider and
   * asserts each manual-clock tick yields a new OAuth token.
   *
   * <p>Repeatedly hit the filesystem around manual-clock sleeps and watch the token set grow. The
   * first call seeds the set, and every subsequent sleep should add exactly one value. Serializing
   * the Delta table configuration mimics executor startup in real workloads. Any mismatch in
   * renewal count would reveal stale-token usage within the provider path.
   */
  @Test
  public void testFileSystemRenewal() throws Exception {
    String location = String.format("gs://%s%s/fs", BUCKET_NAME, dataDir.getCanonicalPath());
    Path locPath = new Path(location);

    sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", TABLE_NAME, location);
    sql("INSERT INTO %s VALUES (1)", TABLE_NAME);

    SerializableConfiguration serialConf =
        new SerializableConfiguration(
            DeltaTable.forName(session, TABLE_NAME).deltaLog().newDeltaHadoopConf());

    List<Row> rows =
        session
            .read()
            .format("delta")
            .table(TABLE_NAME)
            .toJavaRDD()
            .map(
                row -> {
                  Configuration conf = serialConf.value();
                  GcsCredFileSystem fs =
                      (GcsCredFileSystem) FileSystem.get(new URI(location), conf);

                  for (int refreshIndex = 0; refreshIndex < 10; refreshIndex += 1) {
                    fs.getFileStatus(locPath);
                    assertThat(fs.renewalCount()).isEqualTo(refreshIndex);

                    testClock().sleep(Duration.ofMillis(DEFAULT_INTERVAL_MILLIS));

                    fs.getFileStatus(locPath);
                    assertThat(fs.renewalCount()).isEqualTo(refreshIndex + 1);
                  }

                  return RowFactory.create(10);
                })
            .collect();

    assertThat(rows.stream().map(r -> r.getInt(0)).collect(Collectors.toList()))
        .isEqualTo(ImmutableList.of(10));
  }

  /**
   * Drive a full Delta read-transform-write cycle while advancing the manual clock per partition.
   * The sleep occurs inside mapPartitions so renewals happen during active task execution. The
   * final SELECT asserts data integrity after all renewal events have occurred.
   */
  @Test
  public void testDeltaReadWriteRenewal() throws Exception {
    String srcLoc = String.format("gs://%s%s/src", BUCKET_NAME, dataDir.getCanonicalPath());
    String dstLoc = String.format("gs://%s%s/dst", BUCKET_NAME, dataDir.getCanonicalPath());
    String srcTable = String.format("%s.%s.src", CATALOG_NAME, SCHEMA_NAME);
    String dstTable = String.format("%s.%s.dst", CATALOG_NAME, SCHEMA_NAME);

    sql(
        "CREATE TABLE %s (id INT) USING delta LOCATION '%s' PARTITIONED BY (partition INT)",
        srcTable, srcLoc);
    sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", dstTable, dstLoc);
    sql("INSERT INTO %s VALUES (1, 1), (2, 2), (3, 3)", srcTable);

    session
        .read()
        .format("delta")
        .table(srcTable)
        .mapPartitions(
            (MapPartitionsFunction<Row, Integer>)
                input -> {
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
   * Test filesystem that mimics real GCS access while capturing distinct OAuth tokens. Observed
   * tokens are stored in a set so duplicates do not inflate the renewal counter.
   */
  public static class GcsCredFileSystem extends CredentialTestFileSystem {
    private final Set<String> observedTokens = new HashSet<>();
    private final AtomicReference<AccessTokenProvider> providerRef = new AtomicReference<>();

    @Override
    protected String scheme() {
      return GCS_SCHEME;
    }

    /**
     * Filter events so only the configured test bucket records token usage. This keeps unrelated
     * filesystem calls from polluting the observed token set. Each qualifying access fetches the
     * provider and captures its current OAuth token value. Accumulated tokens later drive
     * renewalCount(), which powers the assertions in the test methods.
     */
    @Override
    protected void checkCredentials(Path f) {
      String host = f.toUri().getHost();
      if (!credentialCheckEnabled || !BUCKET_NAME.equals(host)) {
        return;
      }

      AccessTokenProvider provider = accessProvider();
      com.google.cloud.hadoop.util.AccessTokenProvider.AccessToken token =
          provider.getAccessToken();
      assertThat(token).isNotNull();
      assertThat(token.getToken()).isNotNull();
      observedTokens.add(token.getToken());
    }

    public int renewalCount() {
      return Math.max(0, observedTokens.size() - 1);
    }

    /**
     * Cache the provider to avoid reconstructing it on every filesystem call. First, attempt an
     * unsynchronized read for the fast path; return if available. If absent, synchronize and
     * instantiate the configured provider. The provider is instantiated with the Hadoop
     * configuration.
     */
    private AccessTokenProvider accessProvider() {
      AccessTokenProvider existing = providerRef.get();
      if (existing != null) {
        return existing;
      }

      synchronized (providerRef) {
        AccessTokenProvider current = providerRef.get();
        if (current != null) {
          return current;
        }
        try {
          String providerClass = getConf().get("fs.gs.auth.access.token.provider");
          assertThat(providerClass).isEqualTo(GcsVendedTokenProvider.class.getName());
          AccessTokenProvider provider =
              (AccessTokenProvider)
                  Class.forName(providerClass).getDeclaredConstructor().newInstance();
          provider.setConf(getConf());
          providerRef.set(provider);
          return provider;
        } catch (Exception e) {
          throw new RuntimeException("Failed to initialize GCS token provider", e);
        }
      }
    }
  }

  /**
   * Returns a fixed, non-expiring access token for simple integration tests. This allows validating
   * plumbing and downscoping without exercising refresh logic or service-account IO.
   */
  public static class StaticTestingCredentialsGenerator
      implements io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor
          .GcpCredentialsGenerator {
    private final AccessToken staticToken;

    public StaticTestingCredentialsGenerator() {
      this.staticToken =
          AccessToken.newBuilder()
              .setTokenValue("testing://static-token")
              .setExpirationTime(Date.from(Instant.ofEpochMilli(Long.MAX_VALUE)))
              .build();
    }

    @Override
    public AccessToken generate(
        io.unitycatalog.server.service.credential.CredentialContext context) {
      return staticToken;
    }
  }

  /**
   * Issues short-lived tokens whose expiration aligns with the shared manual clock. Each token
   * encodes the active renewal window and bucket so tests can assert rotation frequency. This
   * generator is dynamically loaded by the Unity Catalog server and serves credential generation
   * requests from client REST API calls.
   */
  public static class TestingRenewalCredentialsGenerator
      implements io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor
          .GcpCredentialsGenerator {

    @Override
    public AccessToken generate(
        io.unitycatalog.server.service.credential.CredentialContext context) {
      long currentMillis = testClock().now().toEpochMilli();
      long windowStart = currentMillis / DEFAULT_INTERVAL_MILLIS * DEFAULT_INTERVAL_MILLIS;
      Instant expiration = Instant.ofEpochMilli(windowStart + DEFAULT_INTERVAL_MILLIS);
      String tokenValue =
          String.format("testing-renew://%s#%d", context.getStorageBase(), windowStart);
      return AccessToken.newBuilder()
          .setTokenValue(tokenValue)
          .setExpirationTime(Date.from(expiration))
          .build();
    }
  }
}
