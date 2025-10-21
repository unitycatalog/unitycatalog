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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.SerializableConfiguration;
import org.assertj.core.api.Assertions;
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

  private final File dataDir =
      new File(
          System.getProperty("java.io.tmpdir"), "aws_credential_renewal_test" + UUID.randomUUID());
  private static final AtomicInteger renewedFsCounter = new AtomicInteger(0);

  private static final long DEFAULT_INTERVAL_MILLIS = 30_0000L;
  private static final AtomicInteger ADVANCE_TIMES = new AtomicInteger(0);

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
    serverProperties.put(
        "s3.credentialsGenerator.0", TimeBasedCredentialsGenerator.class.getName());
  }

  private SparkSession createSparkSession() {
    return SparkSession.builder()
        .appName("test-credential-renewal")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop." + UCHadoopConf.UC_MANUAL_CLOCK_NAME, CLOCK_NAME)
        .config("spark.hadoop." + UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 0L)
        .config("spark.default.parallelism", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.catalog.renewal", UCSingleCatalog.class.getName())
        .config("spark.sql.catalog.renewal.uri", serverConfig.getServerUrl())
        .config("spark.sql.catalog.renewal.token", serverConfig.getAuthToken())
        .config("spark.sql.catalog.renewal.warehouse", "renewal")
        .config("spark.sql.catalog.renewal.renewCredential.enabled", "true")
        .config("fs.s3.impl", S3CredRenewFileSystem.class.getName())
        .getOrCreate();
  }

  private SparkSession session;
  private SdkSchemaOperations schemaOperations;

  @BeforeEach
  public void beforeEach() throws Exception {
    super.setUp();

    session = createSparkSession();
    catalogOperations.createCatalog(new CreateCatalog().name("renewal").comment("Spark catalog"));

    schemaOperations = new SdkSchemaOperations(createApiClient(serverConfig));
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
    String location =
        "s3://test-bucket0" + (new File(dataDir, "renewal.default.demo").getCanonicalPath());
    String table = "`renewal`.`default`.`demo`";

    // Create the external delta path-based table.
    S3CredRenewFileSystem.credentialCheckEnabled = false;
    sql("CREATE TABLE delta.`%s` (id INT) USING delta", location);
    S3CredRenewFileSystem.credentialCheckEnabled = true;

    // Create the catalog table referring to external table.
    sql("CREATE TABLE %s USING delta LOCATION '%s'", table, location);

    // Insert the original row.
    sql("INSERT INTO %s VALUES (1)", table);

    SerializableConfiguration serializableConfiguration =
        new SerializableConfiguration(
            DeltaTable.forName(session, table).deltaLog().newDeltaHadoopConf());

    JavaRDD<Row> rdd = session.read().format("delta").table(table).toJavaRDD();
    for (int i = 0; i < 10; i += 1) {
      rdd =
          rdd.map(
              row -> {
                Configuration conf = serializableConfiguration.value();
                FileSystem fs = FileSystem.get(new URI(location), conf);

                fs.getFileStatus(new Path(location)).getLen();

                CLOCK.advance(Duration.ofMillis(DEFAULT_INTERVAL_MILLIS));
                ADVANCE_TIMES.incrementAndGet();

                fs.getFileStatus(new Path(location)).getLen();

                CLOCK.advance(Duration.ofMillis(DEFAULT_INTERVAL_MILLIS));
                ADVANCE_TIMES.incrementAndGet();

                fs.getFileStatus(new Path(location)).getLen();

                CLOCK.advance(Duration.ofMillis(DEFAULT_INTERVAL_MILLIS));
                ADVANCE_TIMES.incrementAndGet();

                return row;
              });
    }

    Dataset<Row> resultDF =
        session.createDataFrame(
            rdd,
            DataTypes.createStructType(
                ImmutableList.of(DataTypes.createStructField("id", DataTypes.IntegerType, true))));

    resultDF.write().format("delta").mode("append").saveAsTable(table);

    assertEquals(
        "Row should be matched",
        IntStream.range(0, 2).mapToObj(i -> row(1)).collect(Collectors.toList()),
        sql("SELECT * FROM %s ORDER BY id ASC", table));

    assertThat(renewedFsCounter.get()).isGreaterThan(1);
  }

  private List<Object[]> sql(String statement, Object... args) {
    List<Row> results = session.sql(String.format(statement, args)).collectAsList();
    if (results.isEmpty()) {
      return ImmutableList.of();
    } else {
      return rowsToJava(results);
    }
  }

  private Object[] row(Object... args) {
    return args;
  }

  protected List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream()
        .map(
            row -> {
              Object[] values = new Object[row.size()];
              for (int i = 0; i < values.length; i++) {
                values[i] = row.getAs(i);
              }
              return values;
            })
        .collect(Collectors.toList());
  }

  protected void assertEquals(
      String context, List<Object[]> expectedRows, List<Object[]> actualRows) {
    Assertions.assertThat(actualRows)
        .as("%s: number of results should match", context)
        .hasSameSizeAs(expectedRows);
    for (int row = 0; row < expectedRows.size(); row += 1) {
      Object[] expected = expectedRows.get(row);
      Object[] actual = actualRows.get(row);
      Assertions.assertThat(actual).as("Number of columns should match").hasSameSizeAs(expected);
      assertEquals(context + ": row " + (row + 1), expected, actual);
    }
  }

  protected void assertEquals(String context, Object[] expectedRow, Object[] actualRow) {
    Assertions.assertThat(actualRow)
        .as("Number of columns should match")
        .hasSameSizeAs(expectedRow);
    for (int col = 0; col < actualRow.length; col += 1) {
      Object expectedValue = expectedRow[col];
      Object actualValue = actualRow[col];
      if (expectedValue != null && expectedValue.getClass().isArray()) {
        String newContext = String.format("%s (nested col %d)", context, col + 1);
        if (expectedValue instanceof byte[]) {
          Assertions.assertThat(actualValue).as(newContext).isEqualTo(expectedValue);
        } else {
          assertEquals(newContext, (Object[]) expectedValue, (Object[]) actualValue);
        }
      } else {
        Assertions.assertThat(actualValue)
            .as("%s contents should match", context)
            .isEqualTo(expectedValue);
      }
    }
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

  public static class S3CredRenewFileSystem extends CredentialTestFileSystem {
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
        if (verifiedTs.size() >= 2) {
          renewedFsCounter.incrementAndGet();
        }
      }
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
