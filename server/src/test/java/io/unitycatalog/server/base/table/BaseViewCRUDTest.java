package io.unitycatalog.server.base.table;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class BaseViewCRUDTest extends BaseTableCRUDTestEnv {

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
    createTestingTable(
        "source_events",
        TableType.EXTERNAL,
        Optional.of(Files.createTempDirectory(testDirectoryRoot, "source").toString()),
        tableOperations);
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
    assertThat(created.getColumns()).hasSize(COLUMNS.size());
    assertThat(created.getTableId()).isNotNull();
    assertThat(created.getStorageLocation()).as("Views should have no storage location").isNull();

    TableInfo fetched = tableOperations.getTable(VIEW_FULL_NAME);
    assertThat(fetched.getName()).isEqualTo(VIEW_NAME);
    assertThat(fetched.getTableType()).isEqualTo(TableType.VIEW);
    assertThat(fetched.getViewDefinition()).isEqualTo(VIEW_DEFINITION);
    assertThat(fetched.getComment()).isEqualTo("A simple SQL view");
    assertThat(fetched.getColumns()).hasSize(COLUMNS.size());
    assertThat(fetched.getCreatedAt()).isNotNull();
    assertThat(fetched.getTableId()).isNotNull();
    assertThat(fetched.getViewDependencies()).isNotNull();
    assertThat(fetched.getViewDependencies().getDependencies()).hasSize(1);
    assertThat(fetched.getViewDependencies().getDependencies().get(0).getTable().getTableFullName())
        .isEqualTo(SOURCE_TABLE_FULL_NAME);

    assertThat(fetched.getProperties()).isNotNull();
    assertThat(fetched.getProperties().get("team")).isEqualTo("analytics");
    assertThat(fetched.getProperties().get("refresh")).isEqualTo("daily");

    List<TableInfo> tables =
        tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, Optional.empty());
    assertThat(tables)
        .as("View should appear in listTables")
        .anyMatch(t -> VIEW_NAME.equals(t.getName()) && TableType.VIEW.equals(t.getTableType()));

    tableOperations.deleteTable(VIEW_FULL_NAME);
    assertApiException(
        () -> tableOperations.getTable(VIEW_FULL_NAME), ErrorCode.TABLE_NOT_FOUND, VIEW_FULL_NAME);
  }

  private static Stream<Arguments> negativeCreateCases() {
    return Stream.of(
        Arguments.of(
            "missing view_definition",
            (UnaryOperator<CreateTable>) request -> request.viewDefinition(null),
            ErrorCode.INVALID_ARGUMENT,
            "view_definition is required for view"),
        Arguments.of(
            "missing view_dependencies",
            (UnaryOperator<CreateTable>) request -> request.viewDependencies(null),
            ErrorCode.INVALID_ARGUMENT,
            "view_dependencies is required for view"),
        Arguments.of(
            "non-existent dependency",
            (UnaryOperator<CreateTable>)
                request ->
                    request.viewDependencies(
                        makeDependencyList(
                            TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".missing")),
            ErrorCode.NOT_FOUND,
            "View dependency does not exist"));
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
