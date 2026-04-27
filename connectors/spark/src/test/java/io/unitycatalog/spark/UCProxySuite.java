package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.ListSchemasResponse;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.MetadataOnlyTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.RelationCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableSummary;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.ViewInfo;
import org.apache.spark.sql.types.DataTypes;
import org.mockito.ArgumentCaptor;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for UCProxy pagination using mocked API responses.
 *
 * <p>UCProxy is a private Scala class, so Java cannot instantiate it directly. We use reflection
 * with dynamic arg resolution to handle constructor changes across branches.
 *
 * <p>End-to-end integration tests that exercise pagination through UCSingleCatalog and a real UC
 * server are in {@link BaseTableReadWriteTest#testListTablesPagination} and {@link
 * SchemaOperationsTest#testListSchemasPagination}.
 */
public class UCProxySuite {

  private static final String CATALOG_NAME = "test_catalog";
  private static final String SCHEMA_NAME = "test_schema";
  private static final String[] NAMESPACE = new String[] {SCHEMA_NAME};

  private TablesApi mockTablesApi;
  private SchemasApi mockSchemasApi;
  private TableCatalog proxy;
  private RelationCatalog proxyRelations;
  private ViewCatalog proxyViews;
  private SupportsNamespaces proxyNs;

  @BeforeEach
  public void setUp() throws Exception {
    mockTablesApi = mock(TablesApi.class);
    mockSchemasApi = mock(SchemasApi.class);
    ApiClient mockApiClient = mock(ApiClient.class);
    TokenProvider mockTokenProvider = mock(TokenProvider.class);
    TemporaryCredentialsApi mockTempCredApi = mock(TemporaryCredentialsApi.class);

    // UCProxy is a private Scala class — Java cannot call `new UCProxy(...)` directly.
    // We use reflection with dynamic arg resolution so the test adapts when the
    // constructor gains or loses boolean flags across branches.
    Map<Class<?>, Object> argsByType = new HashMap<>();
    argsByType.put(URI.class, new URI("http://localhost:8080"));
    argsByType.put(TokenProvider.class, mockTokenProvider);
    argsByType.put(ApiClient.class, mockApiClient);
    argsByType.put(TablesApi.class, mockTablesApi);
    argsByType.put(TemporaryCredentialsApi.class, mockTempCredApi);

    Class<?> proxyClass = Class.forName("io.unitycatalog.spark.UCProxy");
    Constructor<?> ctor = proxyClass.getDeclaredConstructors()[0];
    ctor.setAccessible(true);

    Parameter[] params = ctor.getParameters();
    Object[] args = new Object[params.length];
    for (int i = 0; i < params.length; i++) {
      Class<?> type = params[i].getType();
      if (argsByType.containsKey(type)) {
        args[i] = argsByType.get(type);
      } else if (type == boolean.class) {
        args[i] = false;
      } else {
        args[i] = null;
      }
    }

    Object proxyObj = ctor.newInstance(args);

    proxy = (TableCatalog) proxyObj;
    proxy.initialize(CATALOG_NAME, new CaseInsensitiveStringMap(Collections.emptyMap()));

    // Inject mock schemasApi (initialize() creates a real one from apiClient)
    Field schemasField = proxyClass.getDeclaredField("schemasApi");
    schemasField.setAccessible(true);
    schemasField.set(proxyObj, mockSchemasApi);

    proxyNs = (SupportsNamespaces) proxyObj;
    proxyRelations = (RelationCatalog) proxyObj;
    proxyViews = (ViewCatalog) proxyObj;
  }

  // -- listTables tests --

  @Test
  public void testListTablesSinglePage() throws Exception {
    ListTablesResponse response =
        new ListTablesResponse()
            .tables(List.of(new TableInfo().name("table1"), new TableInfo().name("table2")))
            .nextPageToken(null);
    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull()))
        .thenReturn(response);

    Identifier[] result = proxy.listTables(NAMESPACE);

    assertThat(result).hasSize(2);
    assertThat(result[0]).isEqualTo(Identifier.of(NAMESPACE, "table1"));
    assertThat(result[1]).isEqualTo(Identifier.of(NAMESPACE, "table2"));
  }

  @Test
  public void testListTablesMultiplePages() throws Exception {
    ListTablesResponse page1 =
        new ListTablesResponse()
            .tables(List.of(new TableInfo().name("table1"), new TableInfo().name("table2")))
            .nextPageToken("token1");
    ListTablesResponse page2 =
        new ListTablesResponse()
            .tables(List.of(new TableInfo().name("table3")))
            .nextPageToken(null);

    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull()))
        .thenReturn(page1);
    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), eq("token1")))
        .thenReturn(page2);

    Identifier[] result = proxy.listTables(NAMESPACE);

    assertThat(result).hasSize(3);
    assertThat(result[0]).isEqualTo(Identifier.of(NAMESPACE, "table1"));
    assertThat(result[1]).isEqualTo(Identifier.of(NAMESPACE, "table2"));
    assertThat(result[2]).isEqualTo(Identifier.of(NAMESPACE, "table3"));

    verify(mockTablesApi).listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull());
    verify(mockTablesApi).listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), eq("token1"));
  }

  @Test
  public void testListTablesEmptyResult() throws Exception {
    ListTablesResponse response =
        new ListTablesResponse().tables(Collections.emptyList()).nextPageToken(null);
    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull()))
        .thenReturn(response);

    Identifier[] result = proxy.listTables(NAMESPACE);

    assertThat(result).isEmpty();
  }

  @Test
  public void testListTablesEmptyStringPageTokenStopsPagination() throws Exception {
    ListTablesResponse response =
        new ListTablesResponse().tables(List.of(new TableInfo().name("table1"))).nextPageToken("");
    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull()))
        .thenReturn(response);

    Identifier[] result = proxy.listTables(NAMESPACE);

    assertThat(result).hasSize(1);
    assertThat(result[0]).isEqualTo(Identifier.of(NAMESPACE, "table1"));
  }

  @Test
  public void testListTablesFiltersMetricViews() throws Exception {
    ListTablesResponse response =
        new ListTablesResponse()
            .tables(
                List.of(
                    new TableInfo().name("table1").tableType(TableType.EXTERNAL),
                    new TableInfo().name("mv1").tableType(TableType.METRIC_VIEW)))
            .nextPageToken(null);
    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull()))
        .thenReturn(response);

    Identifier[] result = proxy.listTables(NAMESPACE);

    assertThat(result).containsExactly(Identifier.of(NAMESPACE, "table1"));
  }

  @Test
  public void testListViewsReturnsMetricViewsOnly() throws Exception {
    ListTablesResponse response =
        new ListTablesResponse()
            .tables(
                List.of(
                    new TableInfo().name("table1").tableType(TableType.EXTERNAL),
                    new TableInfo().name("mv1").tableType(TableType.METRIC_VIEW)))
            .nextPageToken(null);
    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull()))
        .thenReturn(response);

    Identifier[] result = proxyViews.listViews(NAMESPACE);

    assertThat(result).containsExactly(Identifier.of(NAMESPACE, "mv1"));
  }

  @Test
  public void testCreateViewSendsMetricViewPayloadAndLoadViewReturnsViewInfo() throws Exception {
    String metadataJson = "{\"metric_view.type\":\"dimension\",\"metric_view.expr\":\"region\"}";
    ViewInfo viewInfo =
        new ViewInfo.Builder()
            .withColumns(
                new Column[] {
                  Column.create("region", DataTypes.StringType, true, null, metadataJson)
                })
            .withProperties(
                Map.of(
                    TableCatalog.PROP_TABLE_TYPE,
                    TableSummary.METRIC_VIEW_TABLE_TYPE,
                    "metric_view.from.type",
                    "ASSET"))
            .withQueryText("version: \"0.1\"\nsource: test_catalog.test_schema.events")
            .withCurrentCatalog(CATALOG_NAME)
            .withCurrentNamespace(NAMESPACE)
            .withViewDependencies(
                org.apache.spark.sql.connector.catalog.DependencyList.of(
                    org.apache.spark.sql.connector.catalog.Dependency.table(
                        "test_catalog.test_schema.events")))
            .build();

    ColumnInfo ucColumn =
        new ColumnInfo()
            .name("region")
            .typeName(ColumnTypeName.STRING)
            .typeText("string")
            .typeJson(
                "{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,"
                    + "\"metadata\":{\"metric_view.type\":\"dimension\","
                    + "\"metric_view.expr\":\"region\"}}")
            .nullable(true)
            .position(0);
    DependencyList ucDeps =
        new DependencyList()
            .dependencies(
                List.of(
                    new Dependency()
                        .table(
                            new TableDependency()
                                .tableFullName("test_catalog.test_schema.events"))));
    TableInfo ucMetricView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mv1")
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition(viewInfo.queryText())
            .columns(List.of(ucColumn))
            .properties(Map.of("metric_view.from.type", "ASSET"))
            .viewDependencies(ucDeps);
    when(mockTablesApi.createTable(any(CreateTable.class))).thenReturn(ucMetricView);
    when(mockTablesApi.getTable(
            eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    ViewInfo loaded = proxyViews.createView(Identifier.of(NAMESPACE, "mv1"), viewInfo);

    ArgumentCaptor<CreateTable> captor = ArgumentCaptor.forClass(CreateTable.class);
    verify(mockTablesApi).createTable(captor.capture());
    CreateTable request = captor.getValue();
    assertThat(request.getTableType()).isEqualTo(TableType.METRIC_VIEW);
    assertThat(request.getViewDefinition()).isEqualTo(viewInfo.queryText());
    assertThat(request.getViewDependencies().getDependencies()).hasSize(1);
    assertThat(request.getColumns()).hasSize(1);
    assertThat(request.getColumns().get(0).getTypeJson()).contains("metric_view.type");
    assertThat(loaded.queryText()).isEqualTo(viewInfo.queryText());
    assertThat(loaded.viewDependencies().dependencies()).hasSize(1);
  }

  @Test
  public void testLoadRelationForMetricViewReturnsMetadataOnlyTableWithViewInfo() throws Exception {
    TableInfo ucMetricView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mv1")
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition("version: \"0.1\"")
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("region")
                        .typeName(ColumnTypeName.STRING)
                        .typeText("string")
                        .typeJson(
                            "{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,"
                                + "\"metadata\":{}}")
                        .nullable(true)
                        .position(0)))
            .properties(Map.of("metric_view.from.type", "ASSET"));
    when(mockTablesApi.getTable(
            eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    org.apache.spark.sql.connector.catalog.Table table =
        proxyRelations.loadRelation(Identifier.of(NAMESPACE, "mv1"));

    assertThat(table).isInstanceOf(MetadataOnlyTable.class);
    ViewInfo view = (ViewInfo) ((MetadataOnlyTable) table).getTableInfo();
    assertThat(view.queryText()).isEqualTo("version: \"0.1\"");
    assertThat(view.properties().get(TableCatalog.PROP_TABLE_TYPE))
        .isEqualTo(TableSummary.METRIC_VIEW_TABLE_TYPE);
    // RelationCatalog spec recommends MetadataOnlyTable.name() be the v2 ident.toString()
    // (quoted multi-part form, no catalog prefix) so DESCRIBE TABLE EXTENDED's `Name` row
    // renders the identifier consistently with every other catalog.
    assertThat(table.name()).isEqualTo(Identifier.of(NAMESPACE, "mv1").toString());
  }

  // -- RelationCatalog cross-type filtering --

  @Test
  public void testLoadTableThrowsNoSuchTableForMetricView() throws Exception {
    TableInfo ucMetricView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mv1")
            .tableType(TableType.METRIC_VIEW);
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    assertThatThrownBy(() -> proxy.loadTable(Identifier.of(NAMESPACE, "mv1")))
        .isInstanceOf(NoSuchTableException.class);
  }

  @Test
  public void testLoadViewThrowsNoSuchViewForRegularTable() throws Exception {
    TableInfo ucTable =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("t1")
            .tableType(TableType.EXTERNAL);
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.t1"), eq(true), eq(true)))
        .thenReturn(ucTable);

    assertThatThrownBy(() -> proxyViews.loadView(Identifier.of(NAMESPACE, "t1")))
        .isInstanceOf(NoSuchViewException.class);
  }

  @Test
  public void testDropTableReturnsFalseForMetricViewAndDoesNotDelete() throws Exception {
    TableInfo ucMetricView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mv1")
            .tableType(TableType.METRIC_VIEW);
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    boolean dropped = proxy.dropTable(Identifier.of(NAMESPACE, "mv1"));

    assertThat(dropped).isFalse();
    // Crucial: deleteTable must not be called, otherwise DROP TABLE on a metric view would
    // silently delete it.
    verify(mockTablesApi, org.mockito.Mockito.never())
        .deleteTable(org.mockito.ArgumentMatchers.anyString());
  }

  @Test
  public void testDropViewReturnsFalseForRegularTableAndDoesNotDelete() throws Exception {
    TableInfo ucTable =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("t1")
            .tableType(TableType.EXTERNAL);
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.t1"), eq(true), eq(true)))
        .thenReturn(ucTable);

    boolean dropped = proxyViews.dropView(Identifier.of(NAMESPACE, "t1"));

    assertThat(dropped).isFalse();
    verify(mockTablesApi, org.mockito.Mockito.never())
        .deleteTable(org.mockito.ArgumentMatchers.anyString());
  }

  @Test
  public void testCreateViewMapsConflictToViewAlreadyExistsException() throws Exception {
    when(mockTablesApi.createTable(any(CreateTable.class)))
        .thenThrow(new ApiException(409, "conflict"));

    ViewInfo viewInfo =
        new ViewInfo.Builder()
            .withColumns(new Column[] {Column.create("c", DataTypes.StringType, true)})
            .withProperties(
                Map.of(TableCatalog.PROP_TABLE_TYPE, TableSummary.METRIC_VIEW_TABLE_TYPE))
            .withQueryText("version: \"0.1\"")
            .withCurrentCatalog(CATALOG_NAME)
            .withCurrentNamespace(NAMESPACE)
            .build();

    assertThatThrownBy(() -> proxyViews.createView(Identifier.of(NAMESPACE, "mv1"), viewInfo))
        .isInstanceOf(ViewAlreadyExistsException.class);
  }

  @Test
  public void testCreateTableMapsConflictToTableAlreadyExistsException() throws Exception {
    when(mockTablesApi.createTable(any(CreateTable.class)))
        .thenThrow(new ApiException(409, "conflict"));

    org.apache.spark.sql.connector.catalog.TableInfo sparkInfo =
        new org.apache.spark.sql.connector.catalog.TableInfo.Builder()
            .withColumns(new Column[] {Column.create("c", DataTypes.StringType, true)})
            .withProperties(Map.of(TableCatalog.PROP_PROVIDER, "parquet"))
            .build();

    assertThatThrownBy(() -> proxy.createTable(Identifier.of(NAMESPACE, "t1"), sparkInfo))
        .isInstanceOf(TableAlreadyExistsException.class);
  }

  // -- listNamespaces tests --

  @Test
  public void testListNamespacesSinglePage() throws Exception {
    ListSchemasResponse response =
        new ListSchemasResponse()
            .schemas(List.of(new SchemaInfo().name("schema1"), new SchemaInfo().name("schema2")))
            .nextPageToken(null);
    when(mockSchemasApi.listSchemas(eq(CATALOG_NAME), eq(0), isNull())).thenReturn(response);

    String[][] result = proxyNs.listNamespaces();

    assertThat(result).hasNumberOfRows(2);
    assertThat(result[0]).containsExactly("schema1");
    assertThat(result[1]).containsExactly("schema2");
  }

  @Test
  public void testListNamespacesMultiplePages() throws Exception {
    ListSchemasResponse page1 =
        new ListSchemasResponse()
            .schemas(List.of(new SchemaInfo().name("schema1"), new SchemaInfo().name("schema2")))
            .nextPageToken("token1");
    ListSchemasResponse page2 =
        new ListSchemasResponse()
            .schemas(List.of(new SchemaInfo().name("schema3")))
            .nextPageToken(null);

    when(mockSchemasApi.listSchemas(eq(CATALOG_NAME), eq(0), isNull())).thenReturn(page1);
    when(mockSchemasApi.listSchemas(eq(CATALOG_NAME), eq(0), eq("token1"))).thenReturn(page2);

    String[][] result = proxyNs.listNamespaces();

    assertThat(result).hasNumberOfRows(3);
    assertThat(result[0]).containsExactly("schema1");
    assertThat(result[1]).containsExactly("schema2");
    assertThat(result[2]).containsExactly("schema3");

    verify(mockSchemasApi).listSchemas(eq(CATALOG_NAME), eq(0), isNull());
    verify(mockSchemasApi).listSchemas(eq(CATALOG_NAME), eq(0), eq("token1"));
  }

  @Test
  public void testListNamespacesEmptyStringPageTokenStopsPagination() throws Exception {
    ListSchemasResponse response =
        new ListSchemasResponse()
            .schemas(List.of(new SchemaInfo().name("schema1")))
            .nextPageToken("");
    when(mockSchemasApi.listSchemas(eq(CATALOG_NAME), eq(0), isNull())).thenReturn(response);

    String[][] result = proxyNs.listNamespaces();

    assertThat(result).hasNumberOfRows(1);
    assertThat(result[0]).containsExactly("schema1");
  }
}
