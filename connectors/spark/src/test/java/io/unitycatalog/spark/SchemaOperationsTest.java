package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
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

  // On Spark 4.2+ (SPARK-55250), this exercises the UCProxy.createNamespace catch
  // clause. On Spark 4.0/4.1, the pre-check in CreateNamespaceExec prevents reaching
  // the catch, but the SQL-level IF NOT EXISTS behavior is still correct.
  @Test
  public void testCreateSchemaIfNotExists() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    session.catalog().setCurrentCatalog(CATALOG_NAME);
    sql("CREATE DATABASE my_ifne_test_db");
    assertThat(session.catalog().databaseExists("my_ifne_test_db")).isTrue();
    sql("CREATE DATABASE IF NOT EXISTS my_ifne_test_db");
    assertThat(session.catalog().databaseExists("my_ifne_test_db")).isTrue();
    sql("DROP DATABASE %s.my_ifne_test_db", CATALOG_NAME);
    assertThat(session.catalog().databaseExists("my_ifne_test_db")).isFalse();
  }

  @Test
  public void testCreateExistingSchemaThrows() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    session.catalog().setCurrentCatalog(CATALOG_NAME);
    sql("CREATE DATABASE my_duplicate_db");
    try {
      assertThatThrownBy(() -> sql("CREATE DATABASE my_duplicate_db"))
          .isInstanceOf(NamespaceAlreadyExistsException.class);
    } finally {
      sql("DROP DATABASE %s.my_duplicate_db", CATALOG_NAME);
    }
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

  @Test
  public void testListSchemasPagination() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    Integer originalPageSize = PagedListingHelper.DEFAULT_PAGE_SIZE;
    try {
      PagedListingHelper.DEFAULT_PAGE_SIZE = 2;
      // Default schema already exists from setUp, create 2 more for 3 total
      sql("CREATE DATABASE %s.pagination_schema_1", SPARK_CATALOG);
      sql("CREATE DATABASE %s.pagination_schema_2", SPARK_CATALOG);
      List<Row> schemas = sql("SHOW SCHEMAS IN %s", SPARK_CATALOG);
      assertThat(schemas).hasSize(3);
      List<String> schemaNames =
          schemas.stream().map(row -> row.getString(0)).sorted().collect(Collectors.toList());
      assertThat(schemaNames)
          .containsExactly("pagination_schema_1", "pagination_schema_2", SCHEMA_NAME);
    } finally {
      PagedListingHelper.DEFAULT_PAGE_SIZE = originalPageSize;
      sql("DROP DATABASE IF EXISTS %s.pagination_schema_1", SPARK_CATALOG);
      sql("DROP DATABASE IF EXISTS %s.pagination_schema_2", SPARK_CATALOG);
    }
  }
}
