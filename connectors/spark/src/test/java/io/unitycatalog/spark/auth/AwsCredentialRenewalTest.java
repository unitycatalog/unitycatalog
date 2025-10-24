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
import io.unitycatalog.server.service.credential.aws.CredentialsGenerator;
import io.unitycatalog.spark.CredentialTestFileSystem;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.UCSingleCatalog;
import io.unitycatalog.spark.utils.Clock;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sparkproject.guava.collect.ImmutableList;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.sts.model.Credentials;

public class AwsCredentialRenewalTest extends BaseCRUDTest {
  private static final String CLOCK_NAME = AwsCredentialRenewalTest.class.getSimpleName();
  private static final Clock CLOCK = Clock.getManualClock(CLOCK_NAME);
  private static final String CREDENTIALS_GENERATOR = TimeBasedCredentialsGenerator.class.getName();
  private static final String TABLE_NAME = "renewal.default.demo";

  private final File dataDir =
      new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

  private static final long DEFAULT_INTERVAL_MILLIS = 30_0000L;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(createApiClient(config));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.put("s3.bucketPath.0", "s3://test-bucket0");
    serverProperties.put("s3.accessKey.0", "accessKey0");
    serverProperties.put("s3.secretKey.0", "secretKey0");
    serverProperties.put("s3.sessionToken.0", "sessionToken0");
    serverProperties.put("s3.credentialsGenerator.0", CREDENTIALS_GENERATOR);
  }

  private SparkSession createSparkSession() {
    return SparkSession.builder()
        .appName("test-credential-renewal")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop." + UCHadoopConf.UC_TEST_CLOCK_NAME, CLOCK_NAME)
        .config("spark.hadoop." + UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 0L)
        .config("spark.default.parallelism", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.catalog.renewal", UCSingleCatalog.class.getName())
        .config("spark.sql.catalog.renewal.uri", serverConfig.getServerUrl())
        .config("spark.sql.catalog.renewal.token", serverConfig.getAuthToken())
        .config("spark.sql.catalog.renewal.warehouse", "renewal")
        .config("spark.sql.catalog.renewal.renewCredential.enabled", "true")
        .config("fs.s3.impl", S3CredFileSystem.class.getName())
        .getOrCreate();
  }

  private SparkSession session;

  @BeforeEach
  public void beforeEach() throws Exception {
    super.setUp();

    session = createSparkSession();
    catalogOperations.createCatalog(new CreateCatalog().name("renewal").comment("Spark catalog"));

    SdkSchemaOperations schemaOperations = new SdkSchemaOperations(createApiClient(serverConfig));
    schemaOperations.createSchema(new CreateSchema().name("default").catalogName("renewal"));
  }

  @AfterEach
  public void afterEach() {
    if (session != null) {
      session.stop();
      session = null;
    }

    // Clear the manual clock.
    Clock.removeManualClock(CLOCK_NAME);

    try {
      catalogOperations.deleteCatalog("renewal", Optional.of(true));
    } catch (Exception e) {
      // Ignore
    }
    super.cleanUp();
  }

  @Test
  public void testRenewal() throws Exception {
    String location = "s3://test-bucket0" + (new File(dataDir, TABLE_NAME).getCanonicalPath());
    Path locPath = new Path(location);
    StructType structType =
        DataTypes.createStructType(
            ImmutableList.of(DataTypes.createStructField("id", DataTypes.IntegerType, true)));

    // Create the external delta path-based table.
    S3CredFileSystem.credentialCheckEnabled = false;
    sql("CREATE TABLE delta.`%s` (id INT) USING delta", location);
    S3CredFileSystem.credentialCheckEnabled = true;

    // Create the catalog table referring to external table.
    sql("CREATE TABLE %s USING delta LOCATION '%s'", TABLE_NAME, location);

    // Insert the original row.
    sql("INSERT INTO %s VALUES (1)", TABLE_NAME);

    SerializableConfiguration serialConf =
        new SerializableConfiguration(
            DeltaTable.forName(session, TABLE_NAME).deltaLog().newDeltaHadoopConf());

    JavaRDD<Row> rdd =
        session
            .read()
            .format("delta")
            .table(TABLE_NAME)
            .toJavaRDD()
            .map(
                row -> {
                  Configuration conf = serialConf.value();
                  S3CredFileSystem fs = (S3CredFileSystem) FileSystem.get(new URI(location), conf);

                  fs.getFileStatus(locPath);
                  assertThat(fs.renewalCount()).isEqualTo(0);

                  // Advance the clock to trigger the 1st renewal.
                  CLOCK.advance(Duration.ofMillis(DEFAULT_INTERVAL_MILLIS));
                  fs.getFileStatus(locPath);
                  assertThat(fs.renewalCount()).isEqualTo(1);

                  // Advance the clock to trigger the 2nd renewal.
                  CLOCK.advance(Duration.ofMillis(DEFAULT_INTERVAL_MILLIS));
                  fs.getFileStatus(locPath);
                  assertThat(fs.renewalCount()).isEqualTo(2);

                  // Advance the clock to trigger the 3rd renewal.
                  CLOCK.advance(Duration.ofMillis(DEFAULT_INTERVAL_MILLIS));
                  fs.getFileStatus(locPath);
                  assertThat(fs.renewalCount()).isEqualTo(3);

                  return RowFactory.create(3);
                });

    session
        .createDataFrame(rdd, structType)
        .write()
        .format("delta")
        .mode("append")
        .saveAsTable(TABLE_NAME);

    List<Row> results = sql("SELECT * FROM %s ORDER BY id ASC", TABLE_NAME);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.stream().map(r -> r.getInt(0)).collect(Collectors.toList()))
        .isEqualTo(ImmutableList.of(1, 3));
  }

  private List<Row> sql(String statement, Object... args) {
    return session.sql(String.format(statement, args)).collectAsList();
  }

  public static class TimeBasedCredentialsGenerator implements CredentialsGenerator {

    @Override
    public Credentials generate(CredentialContext credentialContext) {
      long curTsMillis = CLOCK.now().toEpochMilli();
      // Align it into the window [starTs, starTs + DEFAULT_INTERVAL_MILLIS].
      long startTsMillis = curTsMillis / DEFAULT_INTERVAL_MILLIS * DEFAULT_INTERVAL_MILLIS;
      return Credentials.builder()
          .accessKeyId("accessKeyId" + startTsMillis)
          .secretAccessKey("secretAccessKey" + startTsMillis)
          .sessionToken("sessionToken" + startTsMillis)
          .expiration(Instant.ofEpochMilli(startTsMillis + DEFAULT_INTERVAL_MILLIS))
          .build();
    }
  }

  public static class S3CredFileSystem extends CredentialTestFileSystem {
    private final Set<Long> verifiedTs = new HashSet<>();
    private volatile AwsCredentialsProvider lazyProvider;

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      super.initialize(uri, conf);
    }

    @Override
    protected void checkCredentials(Path f) {
      String host = f.toUri().getHost();

      if (credentialCheckEnabled && "test-bucket0".equals(host)) {
        AwsCredentialsProvider provider = accessProvider();
        assertThat(provider).isNotNull();

        long ts = CLOCK.now().toEpochMilli() / DEFAULT_INTERVAL_MILLIS * DEFAULT_INTERVAL_MILLIS;
        AwsSessionCredentials cred = (AwsSessionCredentials) provider.resolveCredentials();
        assertThat(cred.accessKeyId()).isEqualTo("accessKeyId" + ts);
        assertThat(cred.secretAccessKey()).isEqualTo("secretAccessKey" + ts);
        assertThat(cred.sessionToken()).isEqualTo("sessionToken" + ts);

        verifiedTs.add(ts);
      }
    }

    public int renewalCount() {
      return verifiedTs.size() - 1;
    }

    @Override
    protected String scheme() {
      return "s3:";
    }

    private synchronized AwsCredentialsProvider accessProvider() {
      if (lazyProvider != null) {
        return lazyProvider;
      }

      String clazz = getConf().get(UCHadoopConf.S3A_CREDENTIALS_PROVIDER);
      assertThat(clazz).isEqualTo(AwsVendedTokenProvider.class.getName());
      lazyProvider = new AwsVendedTokenProvider(getConf());

      return lazyProvider;
    }
  }
}
