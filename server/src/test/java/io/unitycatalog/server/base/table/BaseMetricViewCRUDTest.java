package io.unitycatalog.server.base.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.FunctionDependency;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public abstract class BaseMetricViewCRUDTest extends BaseTableCRUDTestEnv {

  protected static final String METRIC_VIEW_NAME = "uc_test_metric_view";
  protected static final String METRIC_VIEW_FULL_NAME =
      TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + METRIC_VIEW_NAME;
  protected static final String SOURCE_TABLE_FULL_NAME =
      TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".source_events";

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
        java.util.Arrays.stream(tableFullNames)
            .map(name -> new Dependency().table(new TableDependency().tableFullName(name)))
            .collect(java.util.stream.Collectors.toList()));
    return depList;
  }

  @Test
  public void testMetricViewCRUD() throws Exception {
    assertThatThrownBy(() -> tableOperations.getTable(METRIC_VIEW_FULL_NAME))
        .isInstanceOf(Exception.class);

    // --- Create with asset source ---
    CreateTable createRequest =
        new CreateTable()
            .name(METRIC_VIEW_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition(VIEW_DEFINITION_ASSET_SOURCE)
            .viewDependencies(makeDependencyList(SOURCE_TABLE_FULL_NAME))
            .comment("Daily event counts by day")
            .properties(PROPERTIES);

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
    assertThatThrownBy(() -> tableOperations.getTable(METRIC_VIEW_FULL_NAME))
        .isInstanceOf(Exception.class);
  }

  @Test
  public void testCreateMetricViewWithoutDefinitionFails() throws Exception {
    CreateTable badRequest =
        new CreateTable()
            .name(METRIC_VIEW_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .tableType(TableType.METRIC_VIEW)
            .viewDependencies(makeDependencyList(SOURCE_TABLE_FULL_NAME));
    assertThatThrownBy(() -> tableOperations.createTable(badRequest)).isInstanceOf(Exception.class);
  }

  @Test
  public void testCreateMetricViewWithoutDependenciesFails() throws Exception {
    CreateTable badRequest =
        new CreateTable()
            .name(METRIC_VIEW_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition(VIEW_DEFINITION_ASSET_SOURCE);
    assertThatThrownBy(() -> tableOperations.createTable(badRequest)).isInstanceOf(Exception.class);
  }

  /**
   * Dependency entry with neither a table nor a function set. Hits {@code DependencyDAO.from}'s
   * "Unsupported dependency type" branch.
   */
  @Test
  public void testCreateMetricViewWithEmptyDependencyFails() throws Exception {
    DependencyList deps = new DependencyList();
    deps.setDependencies(java.util.List.of(new Dependency()));
    CreateTable badRequest =
        new CreateTable()
            .name(METRIC_VIEW_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition(VIEW_DEFINITION_ASSET_SOURCE)
            .viewDependencies(deps);
    assertThatThrownBy(() -> tableOperations.createTable(badRequest)).isInstanceOf(Exception.class);
  }

  /**
   * Dependency with a table full name that is not a three-part name. Hits the "three-part name"
   * validation in {@code DependencyDAO.populateThreePartName}.
   */
  @Test
  public void testCreateMetricViewWithNonThreePartTableNameFails() throws Exception {
    DependencyList deps = new DependencyList();
    deps.setDependencies(
        java.util.List.of(
            new Dependency().table(new TableDependency().tableFullName("schema_only.table"))));
    CreateTable badRequest =
        new CreateTable()
            .name(METRIC_VIEW_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition(VIEW_DEFINITION_ASSET_SOURCE)
            .viewDependencies(deps);
    assertThatThrownBy(() -> tableOperations.createTable(badRequest)).isInstanceOf(Exception.class);
  }

  /**
   * Dependency with a table entry whose {@code table_full_name} is empty. Hits the "must not be
   * null or empty" validation in {@code DependencyDAO.populateThreePartName}.
   */
  @Test
  public void testCreateMetricViewWithEmptyTableFullNameFails() throws Exception {
    DependencyList deps = new DependencyList();
    deps.setDependencies(
        java.util.List.of(new Dependency().table(new TableDependency().tableFullName(""))));
    CreateTable badRequest =
        new CreateTable()
            .name(METRIC_VIEW_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition(VIEW_DEFINITION_ASSET_SOURCE)
            .viewDependencies(deps);
    assertThatThrownBy(() -> tableOperations.createTable(badRequest)).isInstanceOf(Exception.class);
  }

  /**
   * Dependency with a function entry whose {@code function_full_name} is not a three-part name.
   * Hits the same validation as tables but on the function branch.
   */
  @Test
  public void testCreateMetricViewWithNonThreePartFunctionNameFails() throws Exception {
    DependencyList deps = new DependencyList();
    deps.setDependencies(
        java.util.List.of(
            new Dependency().function(new FunctionDependency().functionFullName("just_a_name"))));
    CreateTable badRequest =
        new CreateTable()
            .name(METRIC_VIEW_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition(VIEW_DEFINITION_ASSET_SOURCE)
            .viewDependencies(deps);
    assertThatThrownBy(() -> tableOperations.createTable(badRequest)).isInstanceOf(Exception.class);
  }
}
