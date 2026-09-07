package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.TableInfo;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * Spark 4.2 exposes the v2 {@code ViewCatalog}, so view create/drop route to Unity Catalog through
 * SQL DDL. The shared read guarantees (query-output-column and creation-context round-trips) live
 * in {@link AbstractViewReadIntegrationTest}; this subclass supplies the SQL create/drop and the
 * {@code SHOW VIEWS} check, and adds the 4.2-only dependency-derivation assertions (the 4.0/4.1
 * read-only path creates views via the SDK and never derives dependencies).
 */
public class UCViewDDLIntegrationTest extends AbstractViewReadIntegrationTest {

  @Override
  protected void createView() {
    // Declare DECLARED_COLUMNS over VIEW_QUERY's output (QUERY_OUTPUT_COLUMNS); the names differ,
    // so this exercises the query-output-column round-trip.
    sql(
        "CREATE VIEW %s (%s) AS %s",
        VIEW_FULL_NAME, String.join(", ", DECLARED_COLUMNS), VIEW_QUERY);
  }

  @Override
  protected void dropView() {
    sql("DROP VIEW %s", VIEW_FULL_NAME);
  }

  @Override
  protected void verifyShowViews(boolean expectPresent) {
    List<String> views =
        sql("SHOW VIEWS IN %s.%s", CATALOG_NAME, SCHEMA_NAME).stream()
            .map(r -> r.getString(1))
            .toList();
    if (expectPresent) {
      // SHOW VIEWS lists views only -- the view is present, the source Delta tables are not.
      assertThat(views).contains(VIEW_NAME).doesNotContain(EMPLOYEES_TABLE_NAME);
    } else {
      assertThat(views).doesNotContain(VIEW_NAME);
    }
  }

  /**
   * On the 4.2 create path the connector derives view dependencies from the query text; assert the
   * lifecycle view's two joined base tables were persisted (the unqualified {@code employees}
   * qualified via the creation context, and the cross-catalog {@code departments}).
   */
  @Override
  protected void verifyViewDependencies() {
    assertThat(getServerTable(VIEW_FULL_NAME).getViewDependencies().getDependencies())
        .extracting(dependency -> dependency.getTable().getTableFullName())
        .containsExactlyInAnyOrder(EMPLOYEES_TABLE_FULL_NAME, DEPARTMENTS_TABLE_FULL_NAME);
  }

  /** A case-differing CTE reference is not leaked as a dependency; only the base table is kept. */
  @Test
  public void testCreateViewDerivesDependenciesWithCaseInsensitiveCte() {
    final String DEP_SRC_TABLE_NAME = "dep_src";
    final String DEP_SRC_TABLE_FULL_NAME =
        CATALOG_NAME + "." + SCHEMA_NAME + "." + DEP_SRC_TABLE_NAME;

    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    sql(
        "CREATE TABLE %s (c INT) USING delta LOCATION '%s'",
        DEP_SRC_TABLE_FULL_NAME, testDirectoryRoot.resolve(DEP_SRC_TABLE_NAME).toUri());
    sql(
        "CREATE VIEW %s AS WITH v_cte AS (SELECT * FROM %s) SELECT * FROM V_CTE",
        VIEW_FULL_NAME, DEP_SRC_TABLE_FULL_NAME);
    assertThat(getServerTable(VIEW_FULL_NAME).getViewDependencies().getDependencies())
        .extracting(dependency -> dependency.getTable().getTableFullName())
        .containsExactly(DEP_SRC_TABLE_FULL_NAME);
  }

  @SneakyThrows
  private TableInfo getServerTable(String fullName) {
    return tableOperations.getTable(fullName);
  }
}
