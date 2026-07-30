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

  private static final String STAGING_SCHEMA = "staging";
  private static final String NEUTRAL_SCHEMA = "neutral";

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

  /**
   * A view created with an explicit column list stores the view's output names separately from the
   * query's output names. Spark resolves the view using the persisted query-output names, not the
   * declared column names.
   */
  @Test
  public void testViewWithExplicitColumnListIsReadable() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String view = CATALOG_NAME + "." + SCHEMA_NAME + ".v_renamed";
    sql("CREATE VIEW %s (total) AS SELECT 42 AS my_count", view);

    assertThat(sql("SELECT * FROM %s", view))
        .extracting(row -> row.getInt(0))
        .containsExactly(42);
  }

  /**
   * Unqualified table references in a view body are resolved using the catalog/namespace active at
   * creation time, not the view's own schema. Read from a third schema so the result cannot be
   * explained by the reader's current namespace.
   */
  @Test
  public void testViewResolvesUnqualifiedReferencesUsingCreationContext() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    sql("CREATE SCHEMA IF NOT EXISTS %s.%s", CATALOG_NAME, STAGING_SCHEMA);
    sql("CREATE SCHEMA IF NOT EXISTS %s.%s", CATALOG_NAME, NEUTRAL_SCHEMA);
    sql("CREATE VIEW %s.%s.source AS SELECT 'FROM_STAGING' AS src", CATALOG_NAME, STAGING_SCHEMA);
    sql(
        "CREATE VIEW %s.%s.source AS SELECT 'FROM_DEFAULT' AS src",
        CATALOG_NAME,
        SCHEMA_NAME);

    sql("USE %s.%s", CATALOG_NAME, STAGING_SCHEMA);
    sql(
        "CREATE VIEW %s.%s.v_ctx AS SELECT src FROM source",
        CATALOG_NAME,
        SCHEMA_NAME);

    sql("USE %s.%s", CATALOG_NAME, NEUTRAL_SCHEMA);
    assertThat(sql("SELECT src FROM %s.%s.v_ctx", CATALOG_NAME, SCHEMA_NAME))
        .extracting(row -> row.getString(0))
        .containsExactly("FROM_STAGING");
  }

  private void createSourceTable(String fullName) {
    sql("CREATE TABLE %s (c INT) USING parquet LOCATION '%s'", fullName, sourceTableDir.toURI());
  }

  @SneakyThrows
  private TableInfo getServerTable(String fullName) {
    return new SdkTableOperations(createApiClient(serverConfig)).getTable(fullName);
  }
}
