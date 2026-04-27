package io.unitycatalog.server.base.table;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.FunctionDependency;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class BaseMetricViewCRUDTest extends BaseTableCRUDTestEnv {

  protected static final String METRIC_VIEW_NAME = "uc_test_metric_view";
  protected static final String METRIC_VIEW_FULL_NAME =
      TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + METRIC_VIEW_NAME;
  protected static final String SOURCE_TABLE_FULL_NAME =
      TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".source_events";
  protected static final String SOURCE_FUNCTION_FULL_NAME =
      TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".event_bucket";

  protected static final String VIEW_DEFINITION_ASSET_SOURCE =
      "version: \"0.1\"\n"
          + "source: "
          + SOURCE_TABLE_FULL_NAME
          + "\n"
          + "dimensions:\n"
          + "  - name: event_day\n"
          + "    expr: date_trunc('day', event_time)\n"
          + "measures:\n"
          + "  - name: event_count\n"
          + "    expr: count(*)";

  protected static final Map<String, String> PROPERTIES =
      Map.of("team", "analytics", "refresh", "daily");

  private static DependencyList makeDependencyList(String... tableFullNames) {
    DependencyList depList = new DependencyList();
    depList.setDependencies(
        Arrays.stream(tableFullNames)
            .map(name -> new Dependency().table(new TableDependency().tableFullName(name)))
            .collect(Collectors.toList()));
    return depList;
  }

  private CreateTable validMetricViewRequest() {
    return new CreateTable()
        .name(METRIC_VIEW_NAME)
        .catalogName(TestUtils.CATALOG_NAME)
        .schemaName(TestUtils.SCHEMA_NAME)
        .tableType(TableType.METRIC_VIEW)
        .viewDefinition(VIEW_DEFINITION_ASSET_SOURCE)
        .viewDependencies(makeDependencyList(SOURCE_TABLE_FULL_NAME));
  }

  @Test
  public void testMetricViewCRUD() throws Exception {
    assertApiException(
        () -> tableOperations.getTable(METRIC_VIEW_FULL_NAME),
        ErrorCode.TABLE_NOT_FOUND,
        METRIC_VIEW_FULL_NAME);

    // --- Create with asset source ---
    CreateTable createRequest =
        validMetricViewRequest().comment("Daily event counts by day").properties(PROPERTIES);

    TableInfo created = tableOperations.createTable(createRequest);
    assertThat(created.getName()).isEqualTo(METRIC_VIEW_NAME);
    assertThat(created.getCatalogName()).isEqualTo(TestUtils.CATALOG_NAME);
    assertThat(created.getSchemaName()).isEqualTo(TestUtils.SCHEMA_NAME);
    assertThat(created.getTableType()).isEqualTo(TableType.METRIC_VIEW);
    assertThat(created.getViewDefinition()).isEqualTo(VIEW_DEFINITION_ASSET_SOURCE);
    assertThat(created.getTableId()).isNotNull();
    assertThat(created.getStorageLocation())
        .as("Metric views should have no storage location")
        .isNull();

    // --- Get and verify dependencies round-trip ---
    TableInfo fetched = tableOperations.getTable(METRIC_VIEW_FULL_NAME);
    assertThat(fetched.getName()).isEqualTo(METRIC_VIEW_NAME);
    assertThat(fetched.getTableType()).isEqualTo(TableType.METRIC_VIEW);
    assertThat(fetched.getViewDefinition()).isEqualTo(VIEW_DEFINITION_ASSET_SOURCE);
    assertThat(fetched.getComment()).isEqualTo("Daily event counts by day");
    assertThat(fetched.getCreatedAt()).isNotNull();
    assertThat(fetched.getTableId()).isNotNull();
    assertThat(fetched.getViewDependencies()).isNotNull();
    assertThat(fetched.getViewDependencies().getDependencies()).hasSize(1);
    assertThat(fetched.getViewDependencies().getDependencies().get(0).getTable().getTableFullName())
        .isEqualTo(SOURCE_TABLE_FULL_NAME);

    // Verify properties round-trip
    assertThat(fetched.getProperties()).isNotNull();
    assertThat(fetched.getProperties().get("team")).isEqualTo("analytics");
    assertThat(fetched.getProperties().get("refresh")).isEqualTo("daily");

    // --- List ---
    List<TableInfo> tables =
        tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, Optional.empty());
    assertThat(tables)
        .as("Metric view should appear in listTables")
        .anyMatch(
            t ->
                METRIC_VIEW_NAME.equals(t.getName())
                    && TableType.METRIC_VIEW.equals(t.getTableType()));

    // --- Delete ---
    tableOperations.deleteTable(METRIC_VIEW_FULL_NAME);
    assertApiException(
        () -> tableOperations.getTable(METRIC_VIEW_FULL_NAME),
        ErrorCode.TABLE_NOT_FOUND,
        METRIC_VIEW_FULL_NAME);
  }

  private static Stream<Arguments> negativeCreateCases() {
    return Stream.of(
        Arguments.of(
            "missing view_definition",
            (UnaryOperator<CreateTable>) request -> request.viewDefinition(null),
            ErrorCode.INVALID_ARGUMENT,
            "view_definition is required for metric view"),
        Arguments.of(
            "missing view_dependencies",
            (UnaryOperator<CreateTable>) request -> request.viewDependencies(null),
            ErrorCode.INVALID_ARGUMENT,
            "view_dependencies is required for metric view"),
        Arguments.of(
            "empty dependency list",
            (UnaryOperator<CreateTable>)
                request -> request.viewDependencies(new DependencyList().dependencies(List.of())),
            ErrorCode.INVALID_ARGUMENT,
            "view_dependencies must contain at least one entry for metric view"),
        Arguments.of(
            "dependency entry with neither table nor function set",
            (UnaryOperator<CreateTable>)
                request ->
                    request.viewDependencies(
                        new DependencyList().dependencies(List.of(new Dependency()))),
            ErrorCode.INVALID_ARGUMENT,
            "Unsupported dependency type"),
        Arguments.of(
            "dependency entry with both table and function set",
            (UnaryOperator<CreateTable>)
                request ->
                    request.viewDependencies(
                        new DependencyList()
                            .dependencies(
                                List.of(
                                    new Dependency()
                                        .table(
                                            new TableDependency()
                                                .tableFullName(SOURCE_TABLE_FULL_NAME))
                                        .function(
                                            new FunctionDependency()
                                                .functionFullName(SOURCE_FUNCTION_FULL_NAME))))),
            ErrorCode.INVALID_ARGUMENT,
            "must have exactly one of table or function set"),
        Arguments.of(
            "table dependency with non-three-part full name",
            (UnaryOperator<CreateTable>)
                request ->
                    request.viewDependencies(
                        new DependencyList()
                            .dependencies(
                                List.of(
                                    new Dependency()
                                        .table(
                                            new TableDependency()
                                                .tableFullName("schema_only.table"))))),
            ErrorCode.INVALID_ARGUMENT,
            "must be a three-part name"),
        Arguments.of(
            "table dependency with empty full name",
            (UnaryOperator<CreateTable>)
                request ->
                    request.viewDependencies(
                        new DependencyList()
                            .dependencies(
                                List.of(
                                    new Dependency()
                                        .table(new TableDependency().tableFullName(""))))),
            ErrorCode.INVALID_ARGUMENT,
            "must not be null or empty"),
        Arguments.of(
            "function dependency with non-three-part full name",
            (UnaryOperator<CreateTable>)
                request ->
                    request.viewDependencies(
                        new DependencyList()
                            .dependencies(
                                List.of(
                                    new Dependency()
                                        .function(
                                            new FunctionDependency()
                                                .functionFullName("just_a_name"))))),
            ErrorCode.INVALID_ARGUMENT,
            "must be a three-part name"));
  }

  @ParameterizedTest(name = "createTable rejects metric view with {0}")
  @MethodSource("negativeCreateCases")
  public void testCreateMetricViewNegativeCases(
      String label,
      UnaryOperator<CreateTable> mutator,
      ErrorCode expectedCode,
      String expectedMessageSubstring) {
    CreateTable badRequest = mutator.apply(validMetricViewRequest());
    assertApiException(
        () -> tableOperations.createTable(badRequest), expectedCode, expectedMessageSubstring);
  }

  /**
   * Happy-path coverage for the {@code FUNCTION} branch in {@link
   * io.unitycatalog.server.persist.dao.DependencyDAO#from} and {@link
   * io.unitycatalog.server.persist.dao.DependencyDAO#toDependency}. The negative tests above only
   * exercise the validation error path; this test creates a metric view that depends on both a
   * table and a function, then asserts the dependency list round-trips through {@code getTable} and
   * {@code listTables} with both kinds of entries reconstructed correctly.
   */
  @Test
  public void testMetricViewWithMixedTableAndFunctionDependencies() throws Exception {
    DependencyList mixed =
        new DependencyList()
            .dependencies(
                List.of(
                    new Dependency()
                        .table(new TableDependency().tableFullName(SOURCE_TABLE_FULL_NAME)),
                    new Dependency()
                        .function(
                            new FunctionDependency().functionFullName(SOURCE_FUNCTION_FULL_NAME))));
    CreateTable createRequest = validMetricViewRequest().viewDependencies(mixed);

    try {
      TableInfo created = tableOperations.createTable(createRequest);
      assertThat(created.getTableType()).isEqualTo(TableType.METRIC_VIEW);

      // --- Get: round-trip through DependencyDAO.toDependency ---
      TableInfo fetched = tableOperations.getTable(METRIC_VIEW_FULL_NAME);
      assertThat(fetched.getViewDependencies()).isNotNull();
      List<Dependency> deps = fetched.getViewDependencies().getDependencies();
      assertThat(deps).hasSize(2);

      Optional<Dependency> tableDep = deps.stream().filter(d -> d.getTable() != null).findFirst();
      Optional<Dependency> functionDep =
          deps.stream().filter(d -> d.getFunction() != null).findFirst();

      assertThat(tableDep).as("Mixed dependency list should preserve the TABLE entry").isPresent();
      assertThat(tableDep.get().getTable().getTableFullName()).isEqualTo(SOURCE_TABLE_FULL_NAME);
      assertThat(tableDep.get().getFunction()).isNull();

      assertThat(functionDep)
          .as("Mixed dependency list should preserve the FUNCTION entry")
          .isPresent();
      assertThat(functionDep.get().getFunction().getFunctionFullName())
          .isEqualTo(SOURCE_FUNCTION_FULL_NAME);
      assertThat(functionDep.get().getTable()).isNull();

      // --- List: dependencies are not required on listTables responses, but the metric view
      // itself must still be present and tagged as METRIC_VIEW. ---
      List<TableInfo> tables =
          tableOperations.listTables(
              TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, Optional.empty());
      assertThat(tables)
          .anyMatch(
              t ->
                  METRIC_VIEW_NAME.equals(t.getName())
                      && TableType.METRIC_VIEW.equals(t.getTableType()));
    } finally {
      tableOperations.deleteTable(METRIC_VIEW_FULL_NAME);
    }
  }
}
