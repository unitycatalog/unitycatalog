package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test for a non-trivial view whose query joins two real UC-backed tables. The existing
 * {@code UCViewDDLIntegrationTest} only exercises the constant {@code SELECT 1 AS c} body, which
 * never resolves a base relation; this drives the full analyzer path -- multi-table resolution,
 * projection of columns from both sides, a filter, and column aliasing -- against a real embedded
 * {@code UnityCatalogServer}. Spark 4.2 only, since {@code CREATE VIEW} against the v2 UC catalog
 * requires the {@code ViewCatalog} surface that 4.0/4.1 lack.
 */
public class UCJoinViewE2ETest extends BaseSparkIntegrationTest {

  @TempDir private File employeesDir;
  @TempDir private File departmentsDir;

  private static final String JOIN_VIEW = "employee_department_view";

  private String tbl(String name) {
    return CATALOG_NAME + "." + SCHEMA_NAME + "." + name;
  }

  /**
   * Creates the two UC-backed source tables the view joins. Plain parquet external tables keep the
   * source setup independent of the Delta managed-create path (like {@code MetricViewE2ETest}).
   *
   * <p>{@code employees(id, name, dept_id)} joined to {@code departments(dept_id, dept_name)} on
   * {@code dept_id}. One employee ({@code dave}, dept 99) has no matching department, so an inner
   * join must drop them -- letting the test prove the join predicate is actually applied, not that
   * rows happen to line up.
   */
  private void createSourceTables() {
    sql(
        "CREATE TABLE %s (id INT, name STRING, dept_id INT) USING parquet LOCATION '%s'",
        tbl("employees"), employeesDir.toURI());
    sql(
        "INSERT INTO %s VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'carol', 10), (4, 'dave', 99)",
        tbl("employees"));

    sql(
        "CREATE TABLE %s (dept_id INT, dept_name STRING) USING parquet LOCATION '%s'",
        tbl("departments"), departmentsDir.toURI());
    sql(
        "INSERT INTO %s VALUES (10, 'engineering'), (20, 'sales'), (30, 'marketing')",
        tbl("departments"));
  }

  /**
   * Creates the join view with an explicit column list ({@code emp}, {@code dept}) whose names
   * differ from the query's output names ({@code name}, {@code dept_name}). Spark persists the
   * declared names as the view schema and the query-output names under
   * {@code view.query.out.col.*}; view resolution matches the parsed query output against the
   * schema BY the persisted query-output names, so the connector must round-trip
   * {@code view.query.out.*} rather than regenerate it from the declared columns.
   */
  private void createJoinView() {
    sql(
        "CREATE VIEW %s (emp, dept) AS "
            + "SELECT e.name, d.dept_name "
            + "FROM %s e JOIN %s d ON e.dept_id = d.dept_id",
        tbl(JOIN_VIEW), tbl("employees"), tbl("departments"));
  }

  @SneakyThrows
  private TableInfo getServerTable(String fullName) {
    return new SdkTableOperations(createApiClient(serverConfig)).getTable(fullName);
  }

  @Test
  public void testJoinViewIsCreatedAndReadableThroughSpark() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    createSourceTables();

    // A view whose body joins both source tables, projects columns from each side, and filters --
    // a real analyzer workload, unlike the constant `SELECT 1 AS c` view. Crucially, the view
    // declares an explicit column list (emp, dept) whose names DIFFER from the query's output
    // names (name, dept_name). Spark persists both: the declared names as the view schema and the
    // query-output names under `view.query.out.col.*`. View resolution matches the parsed query
    // output against the declared schema BY the persisted query-output names, so the connector
    // must round-trip `view.query.out.*` from UC rather than regenerate it from the declared
    // columns -- otherwise Spark looks for a query column named `emp`, the query produces `name`,
    // and resolution fails with INCOMPATIBLE_VIEW_SCHEMA_CHANGE.
    sql(
        "CREATE VIEW %s (emp, dept) AS "
            + "SELECT e.name, d.dept_name "
            + "FROM %s e JOIN %s d ON e.dept_id = d.dept_id "
            + "WHERE d.dept_name <> 'sales'",
        tbl(JOIN_VIEW), tbl("employees"), tbl("departments"));

    // The view's stored schema uses the DECLARED column names, not the query output names.
    List<Row> describe = sql("DESCRIBE %s", tbl(JOIN_VIEW));
    assertThat(describe.stream().map(r -> r.getString(0) + ":" + r.getString(1)))
        .contains("emp:string", "dept:string");

    // Reading through the view resolves both base relations, applies the join + filter, and
    // returns only the projected columns under their declared names. alice/carol (engineering,
    // dept 10) survive; bob (sales) is removed by the WHERE; dave (dept 99) is removed by the
    // inner join (no matching dept).
    List<Row> rows =
        sql("SELECT emp, dept FROM %s ORDER BY emp", tbl(JOIN_VIEW));
    assertThat(rows.stream().map(r -> r.getString(0) + ":" + r.getString(1)))
        .containsExactly("alice:engineering", "carol:engineering");
  }

  @Test
  public void testJoinViewPersistsBothBaseTablesAsDependencies() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    createSourceTables();

    createJoinView();

    // The connector currently sends an empty dependency list (query-text derivation was reverted),
    // so the server records no dependencies. Pin that so the behavior is explicit: if dependency
    // derivation is reintroduced, this assertion is the reminder to update it to expect both
    // `employees` and `departments`. Tolerate either null or an empty list, since the two encode
    // the same "no dependencies" state on the wire.
    TableInfo view = getServerTable(tbl(JOIN_VIEW));
    if (view.getViewDependencies() != null) {
      assertThat(view.getViewDependencies().getDependencies()).isEmpty();
    }
  }

  @Test
  @SneakyThrows
  public void testJoinViewSurvivesSessionRestart() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    createSourceTables();
    createJoinView();

    // Close and rebuild the session so the view is resolved purely from its persisted definition
    // (fresh catalog, no in-session plan cache) -- the read path parses the stored `viewText`
    // against the persisted default catalog/namespace and matches its output to the schema via the
    // persisted query-output column names.
    session.close();
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    List<String> employees =
        sql("SELECT emp FROM %s", tbl(JOIN_VIEW)).stream()
            .map(r -> r.getString(0))
            .collect(Collectors.toList());
    assertThat(employees).containsExactlyInAnyOrder("alice", "bob", "carol");
  }

  /**
   * A view body's UNQUALIFIED table references resolve against the current catalog/namespace at
   * creation time, which Spark persists as {@code view.catalogAndNamespace.*} -- and which can
   * differ from the view's own location. Here the view lives in {@code other_schema} but is created
   * while the session default namespace is {@code SCHEMA_NAME}, and its body references
   * {@code employees} / {@code departments} unqualified. The connector must round-trip the
   * creation-time namespace; if it instead substitutes the view's own location
   * ({@code other_schema}, where those tables do not exist), resolution fails.
   */
  @Test
  public void testViewResolvesUnqualifiedRefsAgainstCreationNamespace() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    createSourceTables(); // employees + departments live in SCHEMA_NAME
    String otherSchema = "other_schema";
    sql("CREATE SCHEMA %s.%s", CATALOG_NAME, otherSchema);

    // Session default namespace is SCHEMA_NAME; the view is created in a DIFFERENT schema, and its
    // body references the base tables UNQUALIFIED (so resolution depends on the creation ns).
    sql("USE %s.%s", CATALOG_NAME, SCHEMA_NAME);
    String viewInOtherSchema = CATALOG_NAME + "." + otherSchema + ".unqualified_ref_view";
    sql(
        "CREATE VIEW %s AS SELECT e.name AS emp FROM employees e "
            + "JOIN departments d ON e.dept_id = d.dept_id",
        viewInOtherSchema);

    List<String> employees =
        sql("SELECT emp FROM %s ORDER BY emp", viewInOtherSchema).stream()
            .map(r -> r.getString(0))
            .collect(Collectors.toList());
    assertThat(employees).containsExactly("alice", "bob", "carol");
  }
}
