package io.unitycatalog.spark.auth;

import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.AccessTokenProvider.AccessToken;
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
import java.time.Duration;
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

  private static final String CLOCK_NAME = GcsCredentialRenewalTest.class.getSimpleName();
  private static final String CATALOG_NAME = "CredRenewalCatalog";
  private static final String SCHEMA_NAME = "Default";
  private static final String TABLE_NAME = String.format("%s.%s.demo", CATALOG_NAME, SCHEMA_NAME);
  private static final String BUCKET_NAME = "test-bucket";
  private static final String GCS_SCHEME = "gs:";
  private static final String GCS_TEST_TOKEN_PREFIX = "testing-renew://";
  private static final long DEFAULT_INTERVAL_MILLIS = 30_000L;

  @TempDir private File dataDir;
  private SparkSession session;
  private SdkSchemaOperations schemaOperations;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(createApiClient(serverConfig));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.put("gcs.bucketPath.0", "gs://" + BUCKET_NAME);
    serverProperties.put(
        "gcs.jsonKeyFilePath.0", GCS_TEST_TOKEN_PREFIX + CLOCK_NAME + "/" + BUCKET_NAME);
  }

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

  public static class GcsCredFileSystem extends CredentialTestFileSystem {

    private final Set<String> observedTokens = new HashSet<>();
    private final AtomicReference<AccessTokenProvider> providerRef = new AtomicReference<>();

    @Override
    protected String scheme() {
      return GCS_SCHEME;
    }

    @Override
    protected void checkCredentials(Path f) {
      String host = f.toUri().getHost();
      if (!credentialCheckEnabled || !BUCKET_NAME.equals(host)) {
        return;
      }

      AccessTokenProvider provider = accessProvider();
      AccessToken token = provider.getAccessToken();
      assertThat(token).isNotNull();
      assertThat(token.getToken()).isNotNull();
      observedTokens.add(token.getToken());
    }

    public int renewalCount() {
      return Math.max(0, observedTokens.size() - 1);
    }

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
}
