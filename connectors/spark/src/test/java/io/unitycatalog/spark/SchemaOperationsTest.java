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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class SchemaOperationsTest extends BaseSparkIntegrationTest {
  @AfterEach
  public void restoreLoadDeltaCatalog() {
    // Restore the default in case testNamespaceOps flipped it (and even if it failed mid-way), so
    // other tests still load the Delta catalog.
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(true);
  }

  // Exercises the full namespace lifecycle (create/list/desc/drop) for both delegate paths:
  // Delta catalog loaded (DelegatingCatalogExtension) vs not (UCProxy). Within each round we run
  // the lifecycle against both the UC catalog (CATALOG_NAME) and the session catalog
  // (spark_catalog) in the same session.
  //
  // The Delta flags must be set before the session is created because the delegate is chosen at
  // catalog-init time. DELTA_CATALOG_LOADED is expected to end up equal to loadDeltaCatalog: it is
  // flipped to true only when the flag is on and DeltaCatalog is on the classpath, so asserting it
  // also confirms the delegate really was UCProxy in the Delta-disabled case.
  @ParameterizedTest(name = "loadDeltaCatalog={0}")
  @ValueSource(booleans = {true, false})
  public void testNamespaceOps(boolean loadDeltaCatalog) {
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(loadDeltaCatalog);
    UCSingleCatalog.DELTA_CATALOG_LOADED().set(false);
    session = createSparkSessionWithCatalogs(CATALOG_NAME, SPARK_CATALOG);

    for (String catalog : List.of(CATALOG_NAME, SPARK_CATALOG)) {
      session.catalog().setCurrentCatalog(catalog);

      // createNamespace
      sql("CREATE DATABASE my_test_database");
      assertThat(session.catalog().databaseExists("my_test_database")).isTrue();

      // listNamespaces (SHOW SCHEMAS) -- includes the default schema plus the one we created.
      List<String> schemaNames =
          sql("SHOW SCHEMAS").stream().map(row -> row.getString(0)).collect(Collectors.toList());
      assertThat(schemaNames).contains("my_test_database", SCHEMA_NAME);

      // loadNamespaceMetadata (DESC SCHEMA)
      List<Row> desc = sql("DESC SCHEMA my_test_database");
      assertThat(desc.get(0).getString(0)).isEqualTo("Catalog Name");
      assertThat(desc.get(0).getString(1)).isEqualTo(catalog);

      // dropNamespace
      sql("DROP DATABASE %s.my_test_database", catalog);
      assertThat(session.catalog().databaseExists("my_test_database")).isFalse();
    }

    // See method comment: when Delta is disabled the delegate must be UCProxy (flag stays false).
    assertThat(UCSingleCatalog.DELTA_CATALOG_LOADED().get()).isEqualTo(loadDeltaCatalog);
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
