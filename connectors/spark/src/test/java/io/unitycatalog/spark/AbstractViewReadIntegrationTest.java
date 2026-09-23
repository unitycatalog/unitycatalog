package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME2;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME2;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end check that a UC view is resolvable and readable through a real Spark session on every
 * supported version (the mock suites don't drive Spark's analyzer, so they miss resolution bugs).
 * All tests live here; subclasses supply only {@link #createView()} / {@link #dropView()} (SQL on
 * Spark 4.2, the SDK on 4.0/4.1, which lack the v2 ViewCatalog).
 *
 * <p>The fixture ({@link #VIEW_QUERY}) is deliberately demanding: a literal plus a join of tables
 * in two catalogs (one UNQUALIFIED, one FULLY-QUALIFIED), projecting int / string / array / struct
 * columns, declared with names ({@link #DECLARED_COLUMNS}) that differ from its query output
 * ({@link #QUERY_OUTPUT_COLUMNS}). Created under {@code CATALOG_NAME.SCHEMA_NAME} but read back
 * with a different current catalog, it exercises both creation-context round-trips: the unqualified
 * {@code employees} resolves only via the persisted {@code view.catalogAndNamespace.*}, and the
 * declared columns read only via the persisted {@code view.query.out.*}. Getting either wrong fails
 * with {@code TABLE_OR_VIEW_NOT_FOUND} / {@code INCOMPATIBLE_VIEW_SCHEMA_CHANGE}.
 */
public abstract class AbstractViewReadIntegrationTest extends BaseSparkIntegrationTest {

  protected SdkTableOperations tableOperations;

  @BeforeEach
  public void setUpTableOperations() {
    tableOperations = new SdkTableOperations(createApiClient(serverConfig));
  }

  protected static final String VIEW_NAME = "spark_test_view";
  protected static final String VIEW_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME + "." + VIEW_NAME;

  /** MANAGED Delta table (in CATALOG_NAME.SCHEMA_NAME), referenced UNQUALIFIED in the view. */
  protected static final String EMPLOYEES_TABLE_NAME = "employees";

  protected static final String EMPLOYEES_TABLE_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE_NAME;

  /** EXTERNAL Delta table in a different catalog AND schema, referenced FULLY-QUALIFIED. */
  protected static final String DEPARTMENTS_TABLE_NAME = "departments";

  protected static final String DEPARTMENTS_TABLE_FULL_NAME =
      CATALOG_NAME2 + "." + SCHEMA_NAME2 + "." + DEPARTMENTS_TABLE_NAME;

  /**
   * Names the SELECT list produces (a literal, aliases, passthroughs), spanning an int, a string,
   * an array, and a struct -- all differ from {@link #DECLARED_COLUMNS} below.
   */
  protected static final String[] QUERY_OUTPUT_COLUMNS = {
    "123", "e_id", "emp_name", "budget", "tags", "emp_info"
  };

  /** Declared view column names -- intentionally different from {@link #QUERY_OUTPUT_COLUMNS}. */
  protected static final String[] DECLARED_COLUMNS = {
    "num", "emp", "person", "dept_budget", "dept_tags", "person_info"
  };

  protected static final String VIEW_QUERY =
      "SELECT 123, e.id AS e_id, e.name AS emp_name, d.budget, d.tags, "
          + "named_struct('id', e.id, 'name', e.name) AS emp_info "
          + "FROM "
          + EMPLOYEES_TABLE_NAME
          + " e JOIN "
          + DEPARTMENTS_TABLE_FULL_NAME
          + " d ON e.dept_id = d.dept_id WHERE d.budget > 100";

  /** Creates VIEW_NAME (declaring {@link #DECLARED_COLUMNS} over {@link #VIEW_QUERY}). */
  protected abstract void createView();

  /** Drops VIEW_NAME. */
  protected abstract void dropView();

  /**
   * Asserts whether {@code SHOW VIEWS} lists the view. Default no-op: {@code SHOW VIEWS} only works
   * on Spark 4.2 (4.0/4.1 throw {@code missingCatalogViewsAbilityError}); the 4.2 subclass
   * overrides.
   */
  protected void verifyShowViews(boolean expectPresent) {}

  /**
   * Asserts the view's server-persisted dependencies. Default no-op: only the Spark 4.2 create path
   * derives them from the query text (4.0/4.1 create via the SDK with no derivation); the 4.2
   * subclass overrides to check the two source tables were captured.
   */
  protected void verifyViewDependencies() {}

  /** Creates the MANAGED {@code employees} and EXTERNAL {@code departments} Delta source tables. */
  protected void createSourceTables() {
    sql(
        "CREATE TABLE %s (id INT, name STRING, dept_id INT) USING delta",
        EMPLOYEES_TABLE_FULL_NAME);
    sql(
        "INSERT INTO %s VALUES (1, 'Ann', 10), (2, 'Bob', 20), (3, 'Cy', 10), (4, 'Di', 99)",
        EMPLOYEES_TABLE_FULL_NAME);
    sql("CREATE SCHEMA IF NOT EXISTS %s.%s", CATALOG_NAME2, SCHEMA_NAME2);
    sql(
        "CREATE TABLE %s (dept_id INT, budget INT, tags ARRAY<STRING>) USING delta LOCATION '%s'",
        DEPARTMENTS_TABLE_FULL_NAME, testDirectoryRoot.resolve(DEPARTMENTS_TABLE_NAME).toUri());
    sql(
        "INSERT INTO %s VALUES (10, 500, array('eng', 'core')), (20, 50, array('ops')), "
            + "(30, 700, array('sales'))",
        DEPARTMENTS_TABLE_FULL_NAME);
  }

  protected void createSessionAndView() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME, CATALOG_NAME2);
    createSourceTables();
    // Create under CATALOG_NAME.SCHEMA_NAME so unqualified `employees` binds there -- that becomes
    // the view's captured creation context.
    sql("USE %s.%s", CATALOG_NAME, SCHEMA_NAME);
    createView();
  }

  /**
   * Asserts VIEW_NAME's presence in {@code SHOW TABLES} (which lists views on every version) via
   * the {@code IN catalog.schema} form; when {@code currentIsViewNamespace}, also the unqualified
   * form.
   */
  protected void verifyShowTables(boolean expectPresent, boolean currentIsViewNamespace) {
    assertThat(viewListedBy(sql("SHOW TABLES IN %s.%s", CATALOG_NAME, SCHEMA_NAME)))
        .isEqualTo(expectPresent);
    if (currentIsViewNamespace) {
      assertThat(viewListedBy(sql("SHOW TABLES"))).isEqualTo(expectPresent);
    }
  }

  private static boolean viewListedBy(List<Row> showResult) {
    return showResult.stream().anyMatch(r -> VIEW_NAME.equals(r.getString(1)));
  }

  /**
   * SELECT / DESCRIBE the view (by {@code viewRef}) under its DECLARED column names. The result is
   * catalog-independent, since resolution uses the persisted creation context. Expected rows
   * reflect the filter (budget > 100 drops dept 20) and inner join (drops employee 4, dept 99
   * unmatched).
   */
  private void verifyViewReadable(String viewRef) {
    // Columns: num(int literal), emp(int), person(string), dept_budget(int), dept_tags(array),
    // person_info(struct). getList/getStruct exercise the complex-type round-trip.
    assertThat(
            sql("SELECT * FROM %s ORDER BY %s", viewRef, DECLARED_COLUMNS[1]).stream()
                .map(
                    r ->
                        r.getInt(0)
                            + ":"
                            + r.getInt(1)
                            + ":"
                            + r.getString(2)
                            + ":"
                            + r.getInt(3)
                            + ":"
                            + r.getList(4)
                            + ":"
                            + r.getStruct(5))
                .collect(Collectors.toList()))
        .containsExactly("123:1:Ann:500:[eng, core]:[1,Ann]", "123:3:Cy:500:[eng, core]:[3,Cy]");

    assertThat(sql("DESCRIBE %s", viewRef).stream().map(r -> r.getString(0)))
        .contains(DECLARED_COLUMNS);
  }

  /**
   * All assertions share one session + fixture (expensive to build), running as ordered stages.
   * SHOW / SELECT / DESCRIBE run twice -- view's own namespace current, then a different one -- to
   * prove resolution is catalog-independent. Drop runs last since it destroys the view.
   */
  @Test
  public void testViewLifecycleThroughSpark() {
    createSessionAndView();

    // View's own namespace current: reference it by BARE name to prove unqualified resolution.
    sql("USE %s.%s", CATALOG_NAME, SCHEMA_NAME);
    verifyShowTables(true, /* currentIsViewNamespace= */ true);
    verifyShowViews(true);
    verifyViewDependencies();
    verifyViewReadable(VIEW_NAME);

    // A different catalog AND schema current: owns `departments` but not `employees` or the view.
    sql("USE %s.%s", CATALOG_NAME2, SCHEMA_NAME2);
    verifyShowTables(true, /* currentIsViewNamespace= */ false);
    verifyShowViews(true);
    verifyViewReadable(VIEW_FULL_NAME);

    dropView();
    verifyShowTables(false, /* currentIsViewNamespace= */ false);
    verifyShowViews(false);
  }
}
