package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static io.unitycatalog.spark.DeltaVersionUtils.MIN_DELTA_VERSION_FOR_UC_DELTA_API;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.S3AssumeRole;
import io.unitycatalog.client.model.S3CompatibleRequest;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.service.credential.gcp.TestingCredentialGenerator;
import io.unitycatalog.server.teststorage.TestStorage;
import io.unitycatalog.server.teststorage.TestStorages;
import io.unitycatalog.server.utils.LocalMappingFileOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseSparkIntegrationTest extends BaseCRUDTest {

  protected ArrayList<String> createdCatalogs = new ArrayList<>();
  protected static final String SPARK_CATALOG = "spark_catalog";

  /** The server's Iceberg REST catalog path, served under the UC base path. */
  private static final String ICEBERG_REST_PATH = "/api/2.1/unity-catalog/iceberg";

  /** S3 bucket with no credentials configured on server - for testing SSP fallback. */
  public static final String NO_CREDS_BUCKET = "test-bucket-2-no-creds";

  /** The S3 buckets the tests read and write; Unity Catalog has credentials for each. */
  protected static final List<String> S3_BUCKETS = List.of("test-bucket0", "test-bucket1");

  /**
   * The S3-compatible storage {@code s3://} paths live on, selected by {@code UC_TEST_STORAGE};
   * empty when they are emulated by fake file systems and clients over local files. Resolved on use
   * rather than in a static initializer, so a failed start reports its cause in each suite.
   */
  protected static Optional<TestStorage> s3Storage() {
    return TestStorages.selected();
  }

  /** The storage credential that reaches {@link #s3Storage()}. */
  private static final String S3_CREDENTIAL = "s3_test_storage";

  private SchemaOperations schemaOperations;
  // Each test would create this session. It will be closed automatically.
  protected SparkSession session;

  // Maps the emulated cloud storage root to the test directory when managed storage is on it;
  // registered in setUp() once the server (and its storage root property) is up.
  private LocalMappingFileOperations mappingFileOperations;

  /**
   * True when this suite drives the given catalog as Iceberg's own REST {@code SparkCatalog}
   * against the server's Iceberg REST catalog, rather than through the UC Spark connector (which
   * has no Iceberg support). Per catalog, so one session can mix both (e.g. a Delta join against
   * Iceberg); subclasses flip this, and {@link #createSparkSessionWithCatalogs} is the only
   * consumer, so no subclass needs to re-implement session creation.
   */
  protected boolean isIcebergCatalog(String catalog) {
    return false;
  }

  private void createCommonResources() throws ApiException {
    // Common setup operations such as creating a catalog and schema
    catalogOperations.createCatalog(
        new CreateCatalog().name(CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    createTestCatalog(SPARK_CATALOG);
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(SPARK_CATALOG));
  }

  protected SparkSession createSparkSessionWithCatalogs(String... catalogs) {
    return createSparkSessionWithCatalogs(true, true, catalogs);
  }

  /** The base Spark builder shared by all integration tests (local master, small shuffle width). */
  private SparkSession.Builder newSparkSessionBuilder() {
    return SparkSession.builder()
        .appName("test")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4");
  }

  protected SparkSession createSparkSessionWithCatalogs(
      boolean renewCred, boolean credScopedFsEnabled, String... catalogs) {
    // renewCred / credScopedFsEnabled are UC-connector options with no Iceberg REST analog.
    // A catalog is wired either as an Iceberg REST catalog or through the UC connector
    // (UCSingleCatalog), which serves Delta, Parquet, and the other non-Iceberg formats.
    boolean anyIceberg = false;
    boolean anyUcConnector = false;
    for (String catalog : catalogs) {
      if (isIcebergCatalog(catalog)) {
        anyIceberg = true;
      } else {
        anyUcConnector = true;
      }
    }
    // Both extensions can coexist in one session, which is what lets a single query join a Delta
    // table against an Iceberg table. The UC connector carries Delta support, so its path loads the
    // Delta extension (harmless for the non-Delta formats it also serves, e.g. Parquet).
    List<String> extensions = new ArrayList<>();
    if (anyUcConnector) {
      extensions.add("io.delta.sql.DeltaSparkSessionExtension");
    }
    if (anyIceberg) {
      extensions.add("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    }
    SparkSession.Builder builder =
        newSparkSessionBuilder().config("spark.sql.extensions", String.join(",", extensions));
    for (String catalog : catalogs) {
      builder =
          isIcebergCatalog(catalog)
              ? icebergCatalogConfig(builder, catalog)
              : ucSingleCatalogConfig(builder, catalog, renewCred, credScopedFsEnabled);
      if (!List.of(SPARK_CATALOG, CATALOG_NAME).contains(catalog)) {
        createTestCatalog(catalog);
      }
    }
    // Delta requires spark.sql.catalog.spark_catalog to be set for ANY Delta operation
    // (DeltaLog.checkRequiredConfigurations); otherwise a Delta table -- even in a named UC catalog
    // -- fails with DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG. When the caller did
    // not configure spark_catalog above, point it at Delta's own catalog to satisfy that check.
    // This applies to any UC-connector session (the connector may serve Delta); Iceberg has no such
    // requirement and these suites address tables only through the named UC catalog, so a
    // pure-Iceberg session needs no spark_catalog override and leaves it as the built-in one.
    if (anyUcConnector && !List.of(catalogs).contains(SPARK_CATALOG)) {
      builder.config(
          catalogConfKey(SPARK_CATALOG), "org.apache.spark.sql.delta.catalog.DeltaCatalog");
    }
    return configureCloudFileSystems(builder).getOrCreate();
  }

  /** True when {@code s3://} paths are on real S3-compatible storage rather than emulated. */
  protected static boolean onRealS3Storage() {
    return s3Storage().isPresent();
  }

  /**
   * True when the S3 storage is real but Unity Catalog vends it no keys ({@code
   * server.s3-compatible-storage.static-key-vending=endpoint-only}), so the engine is configured
   * with the storage's own keys.
   */
  protected static boolean usesAmbientS3Keys() {
    return s3Storage().isPresent() && !s3Storage().get().vendsKeys();
  }

  /**
   * Skips a test whose subject is the emulated storage itself (its fixed credentials, fake file
   * systems, or mock clients), which real storage cannot reproduce.
   *
   * @param reason what the test checks of the emulation
   */
  protected static void assumeEmulatedS3Storage(String reason) {
    Assumptions.assumeFalse(onRealS3Storage(), "Checks the emulated S3 storage: " + reason);
  }

  /**
   * Sets the file systems for cloud paths: S3A for {@code s3://} on real storage, otherwise fake
   * file systems over local files that check the credentials Unity Catalog vends. {@code gs://} and
   * {@code abfs://} are always emulated.
   *
   * @param builder the session builder
   * @return the same builder
   */
  protected static SparkSession.Builder configureCloudFileSystems(SparkSession.Builder builder) {
    if (onRealS3Storage()) {
      builder.config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      // Only keys Unity Catalog vends: without them a request fails at once, instead of reaching
      // for keys from the environment or the host's instance metadata.
      builder.config(
          "spark.hadoop.fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider,"
              + "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
      if (usesAmbientS3Keys()) {
        // Unity Catalog vends only the endpoint, so the engine brings the storage's keys, the way
        // an engine set up for that storage would.
        TestStorage storage = s3Storage().get();
        builder
            .config("spark.hadoop.fs.s3a.access.key", storage.accessKeyId())
            .config("spark.hadoop.fs.s3a.secret.key", storage.secretAccessKey())
            .config("spark.hadoop.fs.s3a.endpoint", storage.s3Endpoint())
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("spark.hadoop.fs.s3a.path.style.access", "true");
      }
    } else {
      builder.config("spark.hadoop.fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    }
    builder.config("spark.hadoop.fs.gs.impl", GCSCredentialTestFileSystem.class.getName());
    builder.config("spark.hadoop.fs.abfs.impl", AzureCredentialTestFileSystem.class.getName());
    return builder;
  }

  /** The Spark SQL config key prefix for a catalog, e.g. {@code spark.sql.catalog.<name>}. */
  private static String catalogConfKey(String catalog) {
    return "spark.sql.catalog." + catalog;
  }

  /** Wires one catalog through the UC Spark connector ({@link UCSingleCatalog}), the Delta path. */
  private SparkSession.Builder ucSingleCatalogConfig(
      SparkSession.Builder builder,
      String catalog,
      boolean renewCred,
      boolean credScopedFsEnabled) {
    String catalogConf = catalogConfKey(catalog);
    return builder
        .config(catalogConf, UCSingleCatalog.class.getName())
        .config(catalogConf + "." + OptionsUtil.URI, serverConfig.getServerUrl())
        .config(catalogConf + "." + OptionsUtil.TOKEN, serverConfig.getAuthToken())
        .config(catalogConf + "." + OptionsUtil.WAREHOUSE, catalog)
        .config(catalogConf + "." + OptionsUtil.RENEW_CREDENTIAL_ENABLED, renewCred)
        .config(catalogConf + "." + OptionsUtil.CRED_SCOPED_FS_ENABLED, credScopedFsEnabled);
  }

  /**
   * Wires one catalog as Iceberg's REST {@code SparkCatalog} against the server's Iceberg REST
   * catalog, with the client FileIO following the storage scheme: on S3 the table consumes the
   * credentials and S3 endpoint UC vends, through a mock S3 client ({@code MockS3ClientFactory})
   * when S3 is emulated; otherwise data files read and write locally through {@code HadoopFileIO}.
   */
  private SparkSession.Builder icebergCatalogConfig(SparkSession.Builder builder, String catalog) {
    String catalogConf = catalogConfKey(catalog);
    builder =
        builder
            .config(catalogConf, "org.apache.iceberg.spark.SparkCatalog")
            .config(catalogConf + ".type", "rest")
            .config(catalogConf + ".uri", serverConfig.getServerUrl() + ICEBERG_REST_PATH)
            .config(catalogConf + ".warehouse", catalog)
            .config(catalogConf + ".token", serverConfig.getAuthToken());
    if ("s3".equals(managedStorageCloudScheme())) {
      builder = builder.config(catalogConf + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
      if (usesAmbientS3Keys()) {
        // The table config Unity Catalog returns carries the endpoint but no keys.
        builder =
            builder
                .config(catalogConf + ".s3.access-key-id", s3Storage().get().accessKeyId())
                .config(catalogConf + ".s3.secret-access-key", s3Storage().get().secretAccessKey());
      }
      return onRealS3Storage()
          ? builder
          : builder.config(catalogConf + ".s3.client-factory-impl", FakeS3.MOCK_S3_CLIENT_FACTORY);
    }
    return builder.config(catalogConf + ".io-impl", "org.apache.iceberg.hadoop.HadoopFileIO");
  }

  protected List<Row> sql(String statement, Object... args) {
    return session.sql(String.format(statement, args)).collectAsList();
  }

  /** True when the running Spark is at least {@code major.minor} (e.g. for version-gated tests). */
  protected static boolean isSparkAtLeast(int major, int minor) {
    String[] parts = org.apache.spark.package$.MODULE$.SPARK_VERSION().split("\\.");
    int runMajor = Integer.parseInt(parts[0]);
    int runMinor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
    return runMajor > major || (runMajor == major && runMinor >= minor);
  }

  /**
   * True when managed tables sit on emulated S3 storage, the only cloud scheme the server can read
   * here (server-side cloud-to-local mapping; for Iceberg also client {@code S3FileIO} + mock
   * factory). The server reads that storage for Iceberg metadata and for Delta backfill
   * verification. On real S3-compatible storage the server reads it directly.
   */
  private boolean managedStorageOnEmulatedS3() {
    String scheme = managedStorageCloudScheme();
    if ("s3".equals(scheme)) {
      return !onRealS3Storage();
    }
    // s3 is the only emulated cloud wired for the server; the only other scheme the suites use is
    // local file. Fail loudly if an unwired scheme (e.g. gs/abfs) ever reaches here.
    assertThat(scheme).as("Managed storage scheme").isEqualTo("file");
    return false;
  }

  /**
   * Registers the real S3-compatible storage the way a deployment does: its buckets, an {@code
   * s3_compatible} credential, and an external location per bucket. The credential assumes the
   * storage's role when it has one, and otherwise follows {@code
   * server.s3-compatible-storage.static-key-vending}. The bucket for the no-credentials case gets
   * no external location.
   */
  private void registerS3Storage(TestStorage storage) throws ApiException {
    for (String bucket : S3_BUCKETS) {
      storage.createBucket(bucket);
    }
    storage.createBucket(NO_CREDS_BUCKET);
    ApiClient apiClient = createApiClient(serverConfig);
    new CredentialsApi(apiClient)
        .createCredential(
            new CreateCredentialRequest()
                .name(S3_CREDENTIAL)
                .purpose(CredentialPurpose.STORAGE)
                .s3Compatible(
                    new S3CompatibleRequest()
                        .accessKeyId(storage.accessKeyId())
                        .secretAccessKey(storage.secretAccessKey())
                        .s3Endpoint(storage.s3Endpoint())
                        // Each vend is an STS session of the storage's role, narrowed by the
                        // session policy Unity Catalog sends; without STS the server's
                        // server.s3-compatible-storage.static-key-vending decides whether clients
                        // get the key
                        // pair.
                        .assumeRole(
                            storage
                                .roleArn()
                                .map(arn -> new S3AssumeRole().roleArn(arn))
                                .orElse(null))));
    ExternalLocationsApi externalLocationsApi = new ExternalLocationsApi(apiClient);
    for (String bucket : S3_BUCKETS) {
      externalLocationsApi.createExternalLocation(
          new CreateExternalLocation()
              .name(bucket.replace('-', '_'))
              .url("s3://" + bucket)
              .credentialName(S3_CREDENTIAL));
    }
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    if (managedStorageOnEmulatedS3()) {
      // Map the emulated storage root (s3://bucket/<test dir>) onto that test directory; the
      // client-side mock maps the same object keys to the same absolute local paths.
      mappingFileOperations.mapLocation(
          NormalizedURL.from(tableStorageRoot), testDirectoryRoot.toAbsolutePath());
    }
    // Some Delta Spark functionalities needs testing mode to be turned on so that we can test.
    // Specifically the file CreateDeltaTableCommand.scala in Delta checks for Utils.isTesting
    // before allowing catalog owned table creation.
    System.setProperty("spark.testing", "true");
    schemaOperations = new SdkSchemaOperations(createApiClient(serverConfig));
    try {
      if (s3Storage().isPresent()) {
        registerS3Storage(s3Storage().get());
      }
      createCommonResources();
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * When Iceberg runs on emulated cloud storage, the server itself reads and writes Iceberg
   * metadata for those tables; map that IO onto the test directory while still vending and
   * validating the real credentials, so the server side shares the fake bucket with the client
   * side.
   */
  @Override
  protected FileOperations decorateFileOperations(FileOperations fileOperations) {
    if (managedStorageOnEmulatedS3()) {
      mappingFileOperations =
          new LocalMappingFileOperations(fileOperations, FakeS3.EXPECTED_VENDED_CREDENTIALS);
      return mappingFileOperations;
    }
    return fileOperations;
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // Delta >= 4.3.0 ships UCDeltaCatalogClientImpl / UCDeltaTokenBasedRestClient, so its
    // managed-Delta create / commit / credential paths all go through the UC Delta API. Turn on
    // the server-side enforcement on those matrix entries so we exercise the actual production
    // configuration. Older Delta still has to use the UC-core writes; keep the flag off there.
    if (DeltaVersionUtils.isDeltaAtLeast(MIN_DELTA_VERSION_FOR_UC_DELTA_API)) {
      serverProperties.put(
          ServerProperties.Property.MANAGED_TABLE_USE_DELTA_API_ONLY.getKey(), "true");
    }
    s3Storage()
        .ifPresent(
            storage ->
                serverProperties.put(
                    ServerProperties.Property.S3_COMPATIBLE_STATIC_KEY_VENDING.getKey(),
                    storage.staticKeyVending().value()));
    // Real S3-compatible storage is reached through external locations instead (registerS3Storage).
    if (!onRealS3Storage()) {
      serverProperties.put("s3.bucketPath.0", "s3://test-bucket0");
      serverProperties.put("s3.accessKey.0", "accessKey0");
      serverProperties.put("s3.secretKey.0", "secretKey0");
      serverProperties.put("s3.sessionToken.0", "sessionToken0");
      serverProperties.put("s3.bucketPath.1", "s3://test-bucket1");
      serverProperties.put("s3.accessKey.1", "accessKey1");
      serverProperties.put("s3.secretKey.1", "secretKey1");
      serverProperties.put("s3.sessionToken.1", "sessionToken1");
    }

    serverProperties.put("gcs.bucketPath.0", "gs://test-bucket0");
    serverProperties.put("gcs.jsonKeyFilePath.0", "testing://0");
    serverProperties.put("gcs.credentialGenerator.0", TestingCredentialGenerator.class.getName());
    serverProperties.put("gcs.bucketPath.1", "gs://test-bucket1");
    serverProperties.put("gcs.jsonKeyFilePath.1", "testing://1");
    serverProperties.put("gcs.credentialGenerator.1", TestingCredentialGenerator.class.getName());

    serverProperties.put("adls.storageAccountName.0", "test-bucket0");
    serverProperties.put("adls.tenantId.0", "tenantId0");
    serverProperties.put("adls.clientId.0", "clientId0");
    serverProperties.put("adls.clientSecret.0", "clientSecret0");
    serverProperties.put("adls.testMode.0", "true");
    serverProperties.put("adls.storageAccountName.1", "test-bucket1");
    serverProperties.put("adls.tenantId.1", "tenantId1");
    serverProperties.put("adls.clientId.1", "clientId1");
    serverProperties.put("adls.clientSecret.1", "clientSecret1");
    serverProperties.put("adls.testMode.1", "true");

    if (managedStorageOnEmulatedS3()) {
      // The S3 FileIO config the server vends requires a configured region for the bucket.
      serverProperties.put("s3.region.0", TestUtils.TEST_AWS_REGION);
    }
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(createApiClient(serverConfig));
  }

  @SneakyThrows
  private void createTestCatalog(String catalogName) {
    catalogOperations.createCatalog(
        new CreateCatalog().name(catalogName).comment("Created by BaseSparkIntegrationTest"));
    createdCatalogs.add(catalogName);
  }

  @AfterEach
  public void cleanUp() {
    for (String catalogName : createdCatalogs) {
      try {
        catalogOperations.deleteCatalog(catalogName, Optional.of(true));
      } catch (Exception e) {
        // Ignore
      }
    }
    createdCatalogs.clear();
    try {
      if (session != null) {
        session.close();
        session = null;
      }
    } catch (Exception e) {
      // Ignore
    }
  }
}
