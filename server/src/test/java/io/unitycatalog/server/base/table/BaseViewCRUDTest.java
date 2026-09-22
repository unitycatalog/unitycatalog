package io.unitycatalog.server.base.table;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertHttpApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.UpdateView;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class BaseViewCRUDTest extends BaseTableCRUDTestEnv {

  protected ViewOperations viewOperations;

  protected abstract ViewOperations createViewOperations(ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    viewOperations = createViewOperations(serverConfig);
  }

  protected static final String VIEW_NAME = "uc_test_view";
  protected static final String VIEW_FULL_NAME =
      TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + VIEW_NAME;
  protected static final String SOURCE_TABLE_FULL_NAME =
      TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".source_events";

  protected static final String VIEW_DEFINITION =
      "SELECT as_int, as_string FROM " + SOURCE_TABLE_FULL_NAME;

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

  private CreateTable validViewRequest() {
    return new CreateTable()
        .name(VIEW_NAME)
        .catalogName(TestUtils.CATALOG_NAME)
        .schemaName(TestUtils.SCHEMA_NAME)
        .columns(COLUMNS)
        .tableType(TableType.VIEW)
        .viewDefinition(VIEW_DEFINITION)
        .viewDependencies(makeDependencyList(SOURCE_TABLE_FULL_NAME));
  }

  private void createSourceTable() throws Exception {
    createSourceTable("source_events");
  }

  private void createSourceTable(String name) throws Exception {
    createTestingTable(
        name,
        TableType.EXTERNAL,
        Optional.of(Files.createTempDirectory(testDirectoryRoot, "source").toString()),
        tableOperations);
  }

  @Test
  public void testUpdateView() throws Exception {
    createSourceTable();
    createSourceTable("replacement_events");
    TableInfo created =
        tableOperations.createTable(
            validViewRequest().comment("old comment").properties(Map.of("old", "value")));

    String replacementDependency =
        TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".replacement_events";
    UpdateView update =
        new UpdateView()
            .tableType(TableType.VIEW)
            .columns(List.of(COLUMNS.get(0)))
            .viewDefinition("SELECT as_int FROM " + replacementDependency)
            .viewDependencies(makeDependencyList(replacementDependency))
            .comment("new comment")
            .properties(Map.of("new", "value"));

    TableInfo updated = viewOperations.updateView(VIEW_FULL_NAME, update);

    assertThat(updated.getTableId()).isEqualTo(created.getTableId());
    assertThat(updated.getOwner()).isEqualTo(created.getOwner());
    assertThat(updated.getCreatedAt()).isEqualTo(created.getCreatedAt());
    assertThat(updated.getCreatedBy()).isEqualTo(created.getCreatedBy());
    assertThat(updated.getUpdatedAt()).isGreaterThanOrEqualTo(created.getUpdatedAt());
    assertThat(updated.getComment()).isEqualTo("new comment");
    assertThat(updated.getColumns()).extracting("name").containsExactly("as_int");
    assertThat(updated.getProperties()).containsExactly(Map.entry("new", "value"));
    assertThat(updated.getViewDefinition()).isEqualTo(update.getViewDefinition());
    assertThat(updated.getViewDependencies().getDependencies())
        .extracting(dep -> dep.getTable().getTableFullName())
        .containsExactly(replacementDependency);
    assertThat(tableOperations.getTable(VIEW_FULL_NAME)).isEqualTo(updated);
  }

  @Test
  public void testUpdateViewFailureLeavesExistingViewUnchanged() throws Exception {
    createSourceTable();
    TableInfo created = tableOperations.createTable(validViewRequest());
    UpdateView invalidUpdate =
        new UpdateView()
            .tableType(TableType.VIEW)
            .columns(COLUMNS)
            .viewDefinition("")
            .properties(Map.of("new", "value"));

    assertApiException(
        () -> viewOperations.updateView(VIEW_FULL_NAME, invalidUpdate),
        ErrorCode.INVALID_ARGUMENT,
        "view_definition is required for view");
    assertThat(tableOperations.getTable(VIEW_FULL_NAME)).isEqualTo(created);

    invalidUpdate
        .viewDefinition(VIEW_DEFINITION)
        .viewDependencies(makeDependencyList(SOURCE_TABLE_FULL_NAME + "_missing"));
    assertApiException(
        () -> viewOperations.updateView(VIEW_FULL_NAME, invalidUpdate),
        ErrorCode.NOT_FOUND,
        "View dependency table does not exist: " + SOURCE_TABLE_FULL_NAME + "_missing");
    assertThat(tableOperations.getTable(VIEW_FULL_NAME)).isEqualTo(created);
  }

  @Test
  public void testUpdateViewRejectsMissingOrNullTableType() throws Exception {
    createSourceTable();
    TableInfo created = tableOperations.createTable(validViewRequest());
    for (String typeField : List.of("", "\"table_type\":null,")) {
      var response =
          TestUtils.sendRaw(
              serverConfig,
              "PATCH",
              "/api/2.1/unity-catalog/tables/" + VIEW_FULL_NAME,
              Optional.of("{" + typeField + "\"columns\":[],\"view_definition\":\"SELECT 1\"}"));
      assertHttpApiException(
          response, ErrorCode.INVALID_ARGUMENT, "table_type is required for updating a view");
    }
    assertThat(tableOperations.getTable(VIEW_FULL_NAME)).isEqualTo(created);
  }

  @Test
  public void testUpdateViewRejectsMissingViewTableTargetAndNonViewType() throws Exception {
    UpdateView update =
        new UpdateView().tableType(TableType.VIEW).columns(COLUMNS).viewDefinition("SELECT 1");
    assertApiException(
        () -> viewOperations.updateView(VIEW_FULL_NAME, update),
        ErrorCode.TABLE_NOT_FOUND,
        "View not found");

    createSourceTable();
    assertApiException(
        () -> viewOperations.updateView(SOURCE_TABLE_FULL_NAME, update),
        ErrorCode.TABLE_NOT_FOUND,
        "View not found");

    update.setTableType(TableType.EXTERNAL);
    assertApiException(
        () -> viewOperations.updateView(VIEW_FULL_NAME, update),
        ErrorCode.INVALID_ARGUMENT,
        "requires table_type VIEW or METRIC_VIEW");
  }

  @Test
  public void testViewCRUD() throws Exception {
    assertApiException(
        () -> tableOperations.getTable(VIEW_FULL_NAME), ErrorCode.TABLE_NOT_FOUND, VIEW_FULL_NAME);

    createSourceTable();

    CreateTable createRequest =
        validViewRequest().comment("A simple SQL view").properties(PROPERTIES);

    TableInfo created = tableOperations.createTable(createRequest);
    assertThat(created.getName()).isEqualTo(VIEW_NAME);
    assertThat(created.getCatalogName()).isEqualTo(TestUtils.CATALOG_NAME);
    assertThat(created.getSchemaName()).isEqualTo(TestUtils.SCHEMA_NAME);
    assertThat(created.getTableType()).isEqualTo(TableType.VIEW);
    assertThat(created.getViewDefinition()).isEqualTo(VIEW_DEFINITION);
    assertThat(created.getComment()).isEqualTo("A simple SQL view");
    assertThat(created.getColumns()).hasSize(COLUMNS.size());
    assertThat(created.getCreatedAt()).isNotNull();
    assertThat(created.getTableId()).isNotNull();
    assertThat(created.getStorageLocation()).as("Views should have no storage location").isNull();
    assertThat(created.getProperties()).containsAllEntriesOf(PROPERTIES);
    assertThat(created.getViewDependencies()).isNotNull();
    assertThat(created.getViewDependencies().getDependencies()).hasSize(1);
    assertThat(created.getViewDependencies().getDependencies().get(0).getTable().getTableFullName())
        .isEqualTo(SOURCE_TABLE_FULL_NAME);

    TableInfo fetched = tableOperations.getTable(VIEW_FULL_NAME);
    assertThat(fetched)
        .as("getTable should return the same view that createTable returned")
        .isEqualTo(created);

    List<TableInfo> tables =
        tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, Optional.empty());
    assertThat(tables)
        .as("View should appear in listTables")
        .anyMatch(t -> VIEW_NAME.equals(t.getName()) && TableType.VIEW.equals(t.getTableType()));

    tableOperations.deleteTable(VIEW_FULL_NAME);
    assertApiException(
        () -> tableOperations.getTable(VIEW_FULL_NAME), ErrorCode.TABLE_NOT_FOUND, VIEW_FULL_NAME);

    // view_dependencies is optional for a plain view: a client that does not compute base-table
    // lineage (e.g. Spark for a plain view) may omit it. The view is still created and readable.
    TableInfo createdWithoutDeps =
        tableOperations.createTable(validViewRequest().viewDependencies(null));
    assertThat(createdWithoutDeps.getName()).isEqualTo(VIEW_NAME);
    assertThat(createdWithoutDeps.getTableType()).isEqualTo(TableType.VIEW);
    assertThat(tableOperations.getTable(VIEW_FULL_NAME).getName()).isEqualTo(VIEW_NAME);
    tableOperations.deleteTable(VIEW_FULL_NAME);
  }

  /**
   * Hibernate maps an unannotated String as varchar(255). View properties (user TBLPROPERTIES,
   * Spark view.sqlConfig.*) can exceed that; create/get must round-trip a longer value.
   */
  @Test
  public void testCreateViewAcceptsPropertyValueLongerThanDefaultVarchar() throws Exception {
    createSourceTable();
    String longValue = "x".repeat(300);
    TableInfo created =
        tableOperations.createTable(validViewRequest().properties(Map.of("user.note", longValue)));
    try {
      assertThat(created.getProperties()).containsEntry("user.note", longValue);
      assertThat(tableOperations.getTable(VIEW_FULL_NAME).getProperties())
          .containsEntry("user.note", longValue);
    } finally {
      tableOperations.deleteTable(VIEW_FULL_NAME);
    }
  }

  private static Stream<Arguments> negativeCreateCases() {
    return Stream.of(
        Arguments.of(
            "missing view_definition",
            (UnaryOperator<CreateTable>) request -> request.viewDefinition(null),
            ErrorCode.INVALID_ARGUMENT,
            "view_definition is required for view"),
        Arguments.of(
            "empty column list",
            (UnaryOperator<CreateTable>) request -> request.columns(List.of()),
            ErrorCode.INVALID_ARGUMENT,
            "columns must contain at least one entry for view"),
        Arguments.of(
            // A client that omits `columns` entirely sends null rather than an empty list. This
            // must be rejected the same way, not surface as an internal error.
            "missing columns",
            (UnaryOperator<CreateTable>) request -> request.columns(null),
            ErrorCode.INVALID_ARGUMENT,
            "columns must contain at least one entry for view"),
        Arguments.of(
            "non-existent dependency",
            (UnaryOperator<CreateTable>)
                request ->
                    request.viewDependencies(
                        makeDependencyList(
                            TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".missing")),
            ErrorCode.NOT_FOUND,
            "View dependency table does not exist"));
  }

  @ParameterizedTest(name = "createTable rejects view with {0}")
  @MethodSource("negativeCreateCases")
  public void testCreateViewNegativeCases(
      String label,
      UnaryOperator<CreateTable> mutator,
      ErrorCode expectedCode,
      String expectedMessageSubstring) {
    CreateTable badRequest = mutator.apply(validViewRequest());
    assertApiException(
        () -> tableOperations.createTable(badRequest), expectedCode, expectedMessageSubstring);
  }
}
