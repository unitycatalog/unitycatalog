package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static io.unitycatalog.spark.DeltaVersionUtils.MIN_DELTA_VERSION_FOR_UC_DELTA_API;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.service.credential.gcp.TestingCredentialGenerator;
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
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseSparkIntegrationTest extends BaseCRUDTest {

  protected ArrayList<String> createdCatalogs = new ArrayList<>();
  protected static final String SPARK_CATALOG = "spark_catalog";

  /** The server's Iceberg REST catalog path, served under the UC base path. */
  private static final String ICEBERG_REST_PATH = "/api/2.1/unity-catalog/iceberg";

  /** S3 bucket with no credentials configured on server - for testing SSP fallback. */
  public static final String NO_CREDS_BUCKET = "test-bucket-2-no-creds";

  private SchemaOperations schemaOperations;
  // Each test would create this session. It will be closed automatically.
  protected SparkSession session;

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
  protected SparkSession.Builder newSparkSessionBuilder() {
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
    // Use fake file system for cloud storage so that we can test credentials.
    builder.config("spark.hadoop.fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    builder.config("spark.hadoop.fs.gs.impl", GCSCredentialTestFileSystem.class.getName());
    builder.config("spark.hadoop.fs.abfs.impl", AzureCredentialTestFileSystem.class.getName());
    return builder.getOrCreate();
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
   * catalog, reading and writing data files through Iceberg's {@code HadoopFileIO} on the local
   * warehouse.
   */
  private SparkSession.Builder icebergCatalogConfig(SparkSession.Builder builder, String catalog) {
    String catalogConf = catalogConfKey(catalog);
    return builder
        .config(catalogConf, "org.apache.iceberg.spark.SparkCatalog")
        .config(catalogConf + ".type", "rest")
        .config(catalogConf + ".uri", serverConfig.getServerUrl() + ICEBERG_REST_PATH)
        .config(catalogConf + ".warehouse", catalog)
        .config(catalogConf + ".io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config(catalogConf + ".token", serverConfig.getAuthToken());
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

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    // Some Delta Spark functionalities needs testing mode to be turned on so that we can test.
    // Specifically the file CreateDeltaTableCommand.scala in Delta checks for Utils.isTesting
    // before allowing catalog owned table creation.
    System.setProperty("spark.testing", "true");
    schemaOperations = new SdkSchemaOperations(createApiClient(serverConfig));
    try {
      createCommonResources();
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
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
    serverProperties.put("s3.bucketPath.0", "s3://test-bucket0");
    serverProperties.put("s3.accessKey.0", "accessKey0");
    serverProperties.put("s3.secretKey.0", "secretKey0");
    serverProperties.put("s3.sessionToken.0", "sessionToken0");
    serverProperties.put("s3.bucketPath.1", "s3://test-bucket1");
    serverProperties.put("s3.accessKey.1", "accessKey1");
    serverProperties.put("s3.secretKey.1", "secretKey1");
    serverProperties.put("s3.sessionToken.1", "sessionToken1");

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
