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
import io.unitycatalog.server.utils.TestUtils;
<<<<<<< HEAD
import java.util.*;
=======
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Row;
>>>>>>> 73f3de13 (Fix the name quote bug in UCSingleCatalog. (#1248))
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseSparkIntegrationTest extends BaseCRUDTest {

  protected ArrayList<String> createdCatalogs = new ArrayList<>();
  protected static final String SPARK_CATALOG = "spark_catalog";

  private SchemaOperations schemaOperations;

  private void createCommonResources() throws ApiException {
    // Common setup operations such as creating a catalog and schema
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    createTestCatalog(SPARK_CATALOG);
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(SPARK_CATALOG));
  }

  protected SparkSession createSparkSessionWithCatalogs(String... catalogs) {
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
<<<<<<< HEAD
              .config(catalogConf + ".uri", serverConfig.getServerUrl())
              .config(catalogConf + ".token", serverConfig.getAuthToken())
              .config(catalogConf + ".warehouse", catalog);
=======
              .config(catalogConf + "." + OptionsUtil.URI, serverConfig.getServerUrl())
              .config(catalogConf + "." + OptionsUtil.TOKEN, serverConfig.getAuthToken())
              .config(catalogConf + "." + OptionsUtil.WAREHOUSE, catalog)
              .config(catalogConf + "." + OptionsUtil.RENEW_CREDENTIAL_ENABLED, renewCred);
      if (!List.of(SPARK_CATALOG, CATALOG_NAME).contains(catalog)) {
        createTestCatalog(catalog);
      }
>>>>>>> 73f3de13 (Fix the name quote bug in UCSingleCatalog. (#1248))
    }
    // Use fake file system for cloud storage so that we can test credentials.
    builder.config("fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    builder.config("fs.gs.impl", GCSCredentialTestFileSystem.class.getName());
    builder.config("fs.abfs.impl", AzureCredentialTestFileSystem.class.getName());
    return builder.getOrCreate();
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = new SdkSchemaOperations(createApiClient(serverConfig));
    cleanUp();
    try {
      createCommonResources();
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(createApiClient(serverConfig));
  }

<<<<<<< HEAD
=======
  private void createTestCatalog(String catalogName) {
    try {
      catalogOperations.createCatalog(
          new CreateCatalog().name(catalogName).comment("Created by BaseSparkIntegrationTest"));
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
    createdCatalogs.add(catalogName);
  }

  @AfterEach
>>>>>>> 73f3de13 (Fix the name quote bug in UCSingleCatalog. (#1248))
  @Override
  public void cleanUp() {
    for (String catalogName : createdCatalogs) {
      try {
        catalogOperations.deleteCatalog(catalogName, Optional.of(true));
      } catch (Exception e) {
        // Ignore
      }
    }
<<<<<<< HEAD
=======
    createdCatalogs.clear();
    try {
      if (session != null) {
        session.close();
        session = null;
      }
    } catch (Exception e) {
      // Ignore
    }
>>>>>>> 73f3de13 (Fix the name quote bug in UCSingleCatalog. (#1248))
    super.cleanUp();
  }
}
