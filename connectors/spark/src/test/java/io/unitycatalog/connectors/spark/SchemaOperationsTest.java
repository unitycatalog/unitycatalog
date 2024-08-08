package io.unitycatalog.connectors.spark;

import io.unitycatalog.server.utils.TestUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.junit.jupiter.api.Test;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaOperationsTest extends BaseSparkIntegrationTest {
  @Test
  public void testCreateSchema() {
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
  public void testSetCurrentDB() {
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
  public void testListSchema() {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    Row row = session.sql("SHOW NAMESPACES").collectAsList().get(0);
    assertThat(row.getString(0)).isEqualTo(SCHEMA_NAME);
    assertThatThrownBy(() -> session.sql("SHOW NAMESPACES IN a.b.c").collect())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Multi-layer namespace is not supported in Unity Catalog");
    session.stop();
  }

  @Test
  public void testLoadSchema() {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    Row[] rows = session.sql("DESC NAMESPACE " + SCHEMA_NAME).collect();
    assertThat(rows).hasSize(2);
    assertThat(rows[0].getString(0)).isEqualTo("Catalog Name");
    assertThat(rows[0].getString(1)).isEqualTo(SPARK_CATALOG);
    assertThat(rows[1].getString(0)).isEqualTo("Namespace Name");
    assertThat(rows[1].getString(1)).isEqualTo(SCHEMA_NAME);

    assertThatThrownBy(() -> session.sql("DESC NAMESPACE NonExist").collect())
        .isInstanceOf(NoSuchNamespaceException.class);

    session.stop();
  }
}
