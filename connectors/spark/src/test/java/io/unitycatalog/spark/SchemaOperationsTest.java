package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.junit.jupiter.api.Test;

public class SchemaOperationsTest extends BaseSparkIntegrationTest {
  @Test
  public void testCreateSchema() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME, SPARK_CATALOG);
    session.catalog().setCurrentCatalog(CATALOG_NAME);
    sql("CREATE DATABASE my_test_database;");
    assertThat(session.catalog().databaseExists("my_test_database")).isTrue();
    sql("DROP DATABASE %s.my_test_database;", CATALOG_NAME);
    assertThat(session.catalog().databaseExists("my_test_database")).isFalse();

    session.catalog().setCurrentCatalog(SPARK_CATALOG);
    sql("CREATE DATABASE my_test_database;");
    assertThat(session.catalog().databaseExists("my_test_database")).isTrue();
    sql("DROP DATABASE %s.my_test_database;", SPARK_CATALOG);
    assertThat(session.catalog().databaseExists("my_test_database")).isFalse();
  }

  @Test
  public void testSetCurrentDB() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, TestUtils.CATALOG_NAME);
    session.catalog().setCurrentCatalog(TestUtils.CATALOG_NAME);
    session.catalog().setCurrentDatabase(SCHEMA_NAME);
    session.catalog().setCurrentCatalog(SPARK_CATALOG);
    // TODO: We need to apply a fix on Spark side to use v2 session catalog handle
    // `setCurrentDatabase` when the catalog name is `spark_catalog`.
    // session.catalog().setCurrentDatabase(SCHEMA_NAME);
  }

  @Test
  public void testListSchema() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    Row row = sql("SHOW SCHEMAS").get(0);
    assertThat(row.getString(0)).isEqualTo(SCHEMA_NAME);
    assertThatThrownBy(() -> sql("SHOW SCHEMAS IN a.b.c"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Multi-layer namespace is not supported in Unity Catalog");
  }

  @Test
  public void testLoadSchema() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    List<Row> rows = sql("DESC SCHEMA %s", SCHEMA_NAME);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getString(0)).isEqualTo("Catalog Name");
    assertThat(rows.get(0).getString(1)).isEqualTo(SPARK_CATALOG);
    assertThat(rows.get(1).getString(0)).isEqualTo("Namespace Name");
    assertThat(rows.get(1).getString(1)).isEqualTo(SCHEMA_NAME);

    assertThatThrownBy(() -> sql("DESC NAMESPACE NonExist"))
        .isInstanceOf(NoSuchNamespaceException.class);
  }
}
