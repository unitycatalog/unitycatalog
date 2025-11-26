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
import java.util.*;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
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
              .config(catalogConf + ".uri", serverConfig.getServerUrl())
              .config(catalogConf + ".token", serverConfig.getAuthToken())
              .config(catalogConf + ".warehouse", catalog);
      if (!List.of(SPARK_CATALOG, CATALOG_NAME).contains(catalog)) {
        createTestCatalog(catalog);
      }
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
  @Override
  public void cleanUp() {
    for (String catalogName : createdCatalogs) {
      try {
        catalogOperations.deleteCatalog(catalogName, Optional.of(true));
      } catch (Exception e) {
        // Ignore
      }
    }
    createdCatalogs.clear();
    super.cleanUp();
  }
}
