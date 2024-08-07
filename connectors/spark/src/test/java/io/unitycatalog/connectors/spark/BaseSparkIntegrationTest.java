package io.unitycatalog.connectors.spark;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseSparkIntegrationTest extends BaseCRUDTest {

  protected static final String SPARK_CATALOG = "spark_catalog";
  protected static final String PARQUET_TABLE = "test_parquet";

  @Test
  public void testCreateSchema() throws ApiException {
    SparkSession session = createSparkSessionWithCatalogs(CATALOG_NAME, SPARK_CATALOG);
    session.catalog().setCurrentCatalog(CATALOG_NAME);
    session.sql("CREATE DATABASE my_test_database;");
    assertTrue(session.catalog().databaseExists("my_test_database"));
    session.sql(String.format("DROP DATABASE %s.my_test_database;", CATALOG_NAME));
    assertFalse(session.catalog().databaseExists("my_test_database"));

    session.catalog().setCurrentCatalog(SPARK_CATALOG);
    session.sql("CREATE DATABASE my_test_database;");
    assertTrue(session.catalog().databaseExists("my_test_database"));
    session.sql(String.format("DROP DATABASE %s.my_test_database;", SPARK_CATALOG));
    assertFalse(session.catalog().databaseExists("my_test_database"));
    session.stop();
  }

  @Test
  public void testSetCurrentDB() throws ApiException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG, TestUtils.CATALOG_NAME);
    session.catalog().setCurrentCatalog(TestUtils.CATALOG_NAME);
    session.catalog().setCurrentDatabase(SCHEMA_NAME);
    session.catalog().setCurrentCatalog(SPARK_CATALOG);
    // TODO: We need to apply a fix on Spark side to use v2 session catalog handle
    // `setCurrentDatabase` when the catalog name is `spark_catalog`.
    // session.catalog().setCurrentDatabase(SCHEMA_NAME);
    session.stop();
  }

  @Test
  public void testListNamespace() throws IOException, ApiException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    Row row = session.sql("SHOW NAMESPACES").collectAsList().get(0);
    assertThat(row.getString(0)).isEqualTo(SCHEMA_NAME);
    assertThatThrownBy(() -> session.sql("SHOW NAMESPACES IN a.b.c").collect())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Multi-layer namespace is not supported in Unity Catalog");
    session.stop();
  }

  @Test
  public void testLoadNamespace() throws IOException, ApiException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    Row[] rows = (Row[]) session.sql("DESC NAMESPACE " + SCHEMA_NAME).collect();
    assertThat(rows).hasSize(2);
    assertThat(rows[0].getString(0)).isEqualTo("Catalog Name");
    assertThat(rows[0].getString(1)).isEqualTo(SPARK_CATALOG);
    assertThat(rows[1].getString(0)).isEqualTo("Namespace Name");
    assertThat(rows[1].getString(1)).isEqualTo(SCHEMA_NAME);

    assertThatThrownBy(() -> session.sql("DESC NAMESPACE NonExist").collect())
        .isInstanceOf(NoSuchNamespaceException.class);

    session.stop();
  }

  private SchemaOperations schemaOperations;

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
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
    for (String catalog : catalogs) {
      String catalogConf = "spark.sql.catalog." + catalog;
      builder =
          builder
              .config(catalogConf, UCSingleCatalog.class.getName())
              .config(catalogConf + ".uri", serverConfig.getServerUrl())
              .config(catalogConf + ".token", serverConfig.getAuthToken());
    }
    // Use fake file system for s3:// so that we can test credentials.
    builder.config("fs.s3.impl", CredentialTestFileSystem.class.getName());
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

  @Override
  public void cleanUp() {
    try {
      catalogOperations.deleteCatalog(SPARK_CATALOG, Optional.of(true));
    } catch (Exception e) {
      // Ignore
    }
    super.cleanUp();
  }
}
