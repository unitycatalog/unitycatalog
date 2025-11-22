package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.*;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.service.credential.gcp.TestingCredentialsGenerator;
import io.unitycatalog.server.utils.TestUtils;
import io.unitycatalog.spark.utils.OptionsUtil;
<<<<<<< HEAD
import java.util.*;
=======
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Row;
>>>>>>> 035c61d6 (Refactor ExternalTableReadWriteTest (#1223))
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseSparkIntegrationTest extends BaseCRUDTest {

  protected static final String SPARK_CATALOG = "spark_catalog";

  private SchemaOperations schemaOperations;
  // Each test would create this session. It will be closed automatically.
  protected SparkSession session;

  private void createCommonResources() throws ApiException {
    // Common setup operations such as creating a catalog and schema
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    catalogOperations.createCatalog(
        new CreateCatalog().name(SPARK_CATALOG).comment("Spark catalog"));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(SPARK_CATALOG));
  }

  protected SparkSession createSparkSessionWithCatalogs(String... catalogs) {
    return createSparkSessionWithCatalogs(false, catalogs);
  }

  protected SparkSession createSparkSessionWithCatalogs(boolean renewCred, String... catalogs) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
    for (String catalog : catalogs) {
      String catalogConf = "spark.sql.catalog." + catalog;
      builder =
          builder
              .config(catalogConf, UCSingleCatalog.class.getName())
              .config(catalogConf + "." + OptionsUtil.URI, serverConfig.getServerUrl())
              .config(catalogConf + "." + OptionsUtil.TOKEN, serverConfig.getAuthToken())
              .config(catalogConf + "." + OptionsUtil.WAREHOUSE, catalog)
              .config(catalogConf + "." + OptionsUtil.RENEW_CREDENTIAL_ENABLED, renewCred);
    }
    // Use fake file system for cloud storage so that we can test credentials.
    builder.config("fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    builder.config("fs.gs.impl", GCSCredentialTestFileSystem.class.getName());
    builder.config("fs.abfs.impl", AzureCredentialTestFileSystem.class.getName());
    return builder.getOrCreate();
  }

  protected List<Row> sql(String statement, Object... args) {
    return session.sql(String.format(statement, args)).collectAsList();
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
    serverProperties.put("gcs.credentialsGenerator.0", TestingCredentialsGenerator.class.getName());
    serverProperties.put("gcs.bucketPath.1", "gs://test-bucket1");
    serverProperties.put("gcs.jsonKeyFilePath.1", "testing://1");
    serverProperties.put("gcs.credentialsGenerator.1", TestingCredentialsGenerator.class.getName());

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

  @AfterEach
  @Override
  public void cleanUp() {
    try {
      catalogOperations.deleteCatalog(SPARK_CATALOG, Optional.of(true));
    } catch (Exception e) {
      // Ignore
    }
    try {
      if (session != null) {
        session.close();
        session = null;
      }
    } catch (Exception e) {
      // Ignore
    }
    super.cleanUp();
  }
}
