package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.io.File;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spark 4.2 exposes the v2 {@code ViewCatalog}, so CREATE / SHOW / DROP VIEW all route to Unity
 * Catalog. Complements the shared read guarantee with the full DDL round-trip.
 */
public class UCViewDDLIntegrationTest extends AbstractViewReadIntegrationTest {

  @TempDir private File sourceTableDir;

  @Override
  protected void createView() {
    sql("CREATE VIEW %s AS %s", VIEW_FULL_NAME, VIEW_QUERY);
  }

  @Test
  public void testShowViewsListsViewAndDropRemovesIt() {
    createSessionAndView();
    assertThat(sql("SHOW VIEWS IN %s.%s", CATALOG_NAME, SCHEMA_NAME))
        .anyMatch(row -> VIEW_NAME.equals(row.getString(1)));

    sql("DROP VIEW %s", VIEW_FULL_NAME);
    assertThat(sql("SHOW VIEWS IN %s.%s", CATALOG_NAME, SCHEMA_NAME))
        .noneMatch(row -> VIEW_NAME.equals(row.getString(1)));
  }

  /** A plain view's dependencies, derived from the query text, are persisted on the server. */
  @Test
  public void testCreateViewPersistsDerivedDependencies() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String srcFullName = CATALOG_NAME + "." + SCHEMA_NAME + ".dep_src";
    createSourceTable(srcFullName);
    sql("CREATE VIEW %s AS SELECT * FROM %s", VIEW_FULL_NAME, srcFullName);

    TableInfo view = getServerTable(VIEW_FULL_NAME);
    assertThat(view.getViewDependencies().getDependencies())
        .extracting(dependency -> dependency.getTable().getTableFullName())
        .contains(srcFullName);
  }

  /** A case-differing CTE reference is not leaked as a dependency; only the base table is kept. */
  @Test
  public void testCreateViewDerivesDependenciesWithCaseInsensitiveCte() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String srcFullName = CATALOG_NAME + "." + SCHEMA_NAME + ".dep_src";
    createSourceTable(srcFullName);
    sql(
        "CREATE VIEW %s AS WITH v_cte AS (SELECT * FROM %s) SELECT * FROM V_CTE",
        VIEW_FULL_NAME, srcFullName);

    assertThat(getServerTable(VIEW_FULL_NAME).getViewDependencies().getDependencies())
        .extracting(dependency -> dependency.getTable().getTableFullName())
        .containsExactly(srcFullName);
  }

  private void createSourceTable(String fullName) {
    sql("CREATE TABLE %s (c INT) USING parquet LOCATION '%s'", fullName, sourceTableDir.toURI());
  }

  @SneakyThrows
  private TableInfo getServerTable(String fullName) {
    return new SdkTableOperations(createApiClient(serverConfig)).getTable(fullName);
  }
}
