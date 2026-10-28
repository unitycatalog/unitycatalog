package io.unitycatalog.server.base.table;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertHttpApiException;
import static io.unitycatalog.server.utils.TestUtils.assertSameIdentity;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.ReplaceView;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
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
  private static final String EXISTING_VIEW_NAME = "uc_existing_view";
  private static final String EXISTING_VIEW_FULL_NAME =
      TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + EXISTING_VIEW_NAME;

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

  private static ReplaceView toReplaceView(CreateTable request) {
    return new ReplaceView()
        .tableType(request.getTableType())
        .columns(request.getColumns())
        .comment(request.getComment())
        .properties(request.getProperties())
        .viewDefinition(request.getViewDefinition())
        .viewDependencies(request.getViewDependencies());
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
  public void testReplaceView() throws Exception {
    createSourceTable();
    createSourceTable("replacement_events");
    TableInfo created =
        tableOperations.createTable(
            validViewRequest().comment("old comment").properties(Map.of("old", "value")));

    String replacementDependency =
        TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".replacement_events";
    ReplaceView replace =
        new ReplaceView()
            .tableType(TableType.VIEW)
            .columns(List.of(COLUMNS.get(0)))
            .viewDefinition("SELECT as_int FROM " + replacementDependency)
            .viewDependencies(makeDependencyList(replacementDependency))
            .comment("new comment")
            .properties(Map.of("new", "value"));

    TableInfo replaced = viewOperations.replaceView(VIEW_FULL_NAME, replace);

    assertSameIdentity(created, replaced);
    assertThat(replaced.getUpdatedAt()).isGreaterThanOrEqualTo(created.getUpdatedAt());
    assertThat(replaced.getComment()).isEqualTo("new comment");
    assertThat(replaced.getColumns()).extracting(ColumnInfo::getName).containsExactly("as_int");
    assertThat(replaced.getProperties()).containsExactly(Map.entry("new", "value"));
    assertThat(replaced.getViewDefinition()).isEqualTo(replace.getViewDefinition());
    assertThat(replaced.getViewDependencies().getDependencies())
        .extracting(dep -> dep.getTable().getTableFullName())
        .containsExactly(replacementDependency);
    assertThat(tableOperations.getTable(VIEW_FULL_NAME)).isEqualTo(replaced);
  }

  @Test
  public void testReplaceViewRejectsMissingOrNullTableType() throws Exception {
    createSourceTable();
    TableInfo created = tableOperations.createTable(validViewRequest());
    String validFields =
        "\"columns\":"
            + new ObjectMapper().writeValueAsString(COLUMNS)
            + ",\"view_definition\":\"SELECT 1\"";
    // Raw HTTP: the generated SDK cannot send a missing or null required field.
    for (String typeField : List.of("", "\"table_type\":null,")) {
      var response =
          TestUtils.sendRaw(
              serverConfig,
              "PUT",
              "/api/2.1/unity-catalog/tables/" + VIEW_FULL_NAME,
              Optional.of("{" + typeField + validFields + "}"));
      assertHttpApiException(
          response, ErrorCode.INVALID_ARGUMENT, "table_type is required for replacing a view");
    }
    assertThat(tableOperations.getTable(VIEW_FULL_NAME)).isEqualTo(created);
  }

  /**
   * Targets set up by {@link #testReplaceViewRejectsTarget}: the EXTERNAL source table from {@link
   * #createSourceTable()} and an existing VIEW.
   */
  private static Stream<Arguments> replaceTargetCases() {
    return Stream.of(
        // A non-view fails the not-a-view check before the type check. The Spark connector relies
        // on that order: it maps FAILED_PRECONDITION (not INVALID_ARGUMENT) to NoSuchViewException.
        Arguments.of(
            "the EXTERNAL source table",
            SOURCE_TABLE_FULL_NAME,
            TableType.VIEW,
            ErrorCode.FAILED_PRECONDITION,
            "Cannot replace " + SOURCE_TABLE_FULL_NAME + ": it is a EXTERNAL, not a view"),
        Arguments.of(
            "a VIEW as METRIC_VIEW",
            EXISTING_VIEW_FULL_NAME,
            TableType.METRIC_VIEW,
            ErrorCode.INVALID_ARGUMENT,
            "Cannot replace VIEW " + EXISTING_VIEW_FULL_NAME + " with table_type METRIC_VIEW"),
        Arguments.of(
            "a VIEW as EXTERNAL",
            EXISTING_VIEW_FULL_NAME,
            TableType.EXTERNAL,
            ErrorCode.INVALID_ARGUMENT,
            "Cannot replace VIEW " + EXISTING_VIEW_FULL_NAME + " with table_type EXTERNAL"));
  }

  @Test
  public void testReplaceMissingViewIsNotFound() {
    assertApiException(
        () -> viewOperations.replaceView(VIEW_FULL_NAME, toReplaceView(validViewRequest())),
        ErrorCode.TABLE_NOT_FOUND,
        "Table not found: " + VIEW_FULL_NAME);
  }

  @ParameterizedTest(name = "replaceView rejects {0} and leaves it unchanged")
  @MethodSource("replaceTargetCases")
  public void testReplaceViewRejectsTarget(
      String label,
      String target,
      TableType tableType,
      ErrorCode expectedCode,
      String expectedMessage)
      throws Exception {
    createSourceTable();
    tableOperations.createTable(validViewRequest().name(EXISTING_VIEW_NAME));
    TableInfo before = tableOperations.getTable(target);
    ReplaceView replace = toReplaceView(validViewRequest()).tableType(tableType);

    assertApiException(
        () -> viewOperations.replaceView(target, replace), expectedCode, expectedMessage);
    assertThat(tableOperations.getTable(target)).isEqualTo(before);
  }

  @Test
  public void testReplaceViewClearsOmittedMetadata() throws Exception {
    createSourceTable();
    tableOperations.createTable(
        validViewRequest().comment("old comment").properties(Map.of("old", "value")));

    TableInfo replaced =
        viewOperations.replaceView(
            VIEW_FULL_NAME,
            new ReplaceView()
                .tableType(TableType.VIEW)
                .columns(COLUMNS)
                .viewDefinition(VIEW_DEFINITION));

    assertThat(replaced.getComment()).isNull();
    assertThat(replaced.getProperties()).isEmpty();
    assertThat(replaced.getViewDependencies().getDependencies()).isEmpty();
    assertThat(tableOperations.getTable(VIEW_FULL_NAME)).isEqualTo(replaced);
  }

  @Test
  public void testViewCRUD() throws Exception {
    assertApiException(
        () -> tableOperations.getTable(VIEW_FULL_NAME), ErrorCode.TABLE_NOT_FOUND, VIEW_FULL_NAME);

    createSourceTable();

    CreateTable createRequest =
        validViewRequest()
            .comment("A simple SQL view")
            .properties(PROPERTIES)
            // Out of position order: reads must still return columns in position order.
            .columns(List.of(COLUMNS.get(1), COLUMNS.get(0)));

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
        .usingRecursiveComparison()
        .ignoringFields("columns")
        .isEqualTo(created);
    // The server lowercases typeText on create.
    assertThat(fetched.getColumns())
        .usingRecursiveComparison()
        .withComparatorForFields(String.CASE_INSENSITIVE_ORDER, "typeText")
        .isEqualTo(COLUMNS);

    List<TableInfo> tables =
        tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, Optional.empty());
    assertThat(tables)
        .as("View should appear in listTables")
        .anyMatch(t -> VIEW_NAME.equals(t.getName()) && TableType.VIEW.equals(t.getTableType()));
    // listTables loads properties and view dependencies for the whole page at once; each entry
    // must still carry exactly its own, as getTable returns them.
    assertThat(tables)
        .as("listTables should return the view with its own properties and dependencies")
        .contains(fetched);
    assertThat(tables)
        .as("listTables should return the source table with its own properties")
        .contains(tableOperations.getTable(SOURCE_TABLE_FULL_NAME));

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

  @ParameterizedTest(name = "replaceView rejects view with {0} and leaves it unchanged")
  @MethodSource("negativeCreateCases")
  public void testReplaceViewNegativeCases(
      String label,
      UnaryOperator<CreateTable> mutator,
      ErrorCode expectedCode,
      String expectedMessageSubstring)
      throws Exception {
    createSourceTable();
    TableInfo created = tableOperations.createTable(validViewRequest());
    ReplaceView badRequest = toReplaceView(mutator.apply(validViewRequest()));

    assertApiException(
        () -> viewOperations.replaceView(VIEW_FULL_NAME, badRequest),
        expectedCode,
        expectedMessageSubstring);
    assertThat(tableOperations.getTable(VIEW_FULL_NAME)).isEqualTo(created);
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
