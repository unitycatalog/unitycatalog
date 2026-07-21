package io.unitycatalog.spark;

import static io.unitycatalog.spark.UCProxyTestFixture.CATALOG_NAME;
import static io.unitycatalog.spark.UCProxyTestFixture.NAMESPACE;
import static io.unitycatalog.spark.UCProxyTestFixture.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Relation;
import org.apache.spark.sql.connector.catalog.RelationCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableSummary;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Spark-4.2-only view-side unit tests for {@code UCProxy}. Composes the shared
 * {@link UCProxyTestFixture} (which builds the mock-backed proxy) and adds the
 * {@code RelationCatalog} / {@code ViewCatalog} casts plus the view CRUD / metric-view tests.
 *
 * <p>This file lives under {@code src/test/scala-shims/spark-4.2/}, so the cross-Spark build only
 * compiles and runs it for the Spark 4.2 build, where {@code UCProxy} actually mixes in the V2 view
 * catalog surface ({@code Relation}, {@code View}, {@code RelationCatalog},
 * {@code ViewCatalog}, the V2 {@code TableInfo}). The version-agnostic table/namespace tests live
 * in {@code UCProxySuite} under {@code src/test/java/} and run on all Spark versions.
 *
 * <p>The Spark V2 {@code Dependency} / {@code DependencyList} / {@code TableDependency} /
 * {@code TableInfo} types are used fully-qualified to disambiguate from the UC-client homonyms
 * imported above.
 */
public class UCViewProxySuite {

  private TablesApi mockTablesApi;
  private TableCatalog proxy;
  private RelationCatalog proxyRelations;
  private ViewCatalog proxyViews;

  @BeforeEach
  public void setUp() throws Exception {
    UCProxyTestFixture fixture = new UCProxyTestFixture().build();
    mockTablesApi = fixture.mockTablesApi;
    proxy = fixture.proxy;
    // These casts succeed only on the Spark 4.2 build, where UCProxy's UCProxyViewSupport mixes in
    // RelationCatalog (which extends ViewCatalog). On 4.0/4.1 they would throw, which is why this
    // suite lives in the 4.2-only test source dir.
    proxyRelations = (RelationCatalog) fixture.proxyObj;
    proxyViews = (ViewCatalog) fixture.proxyObj;
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
  public void testCreateViewSendsMetricViewPayloadAndLoadViewReturnsView() throws Exception {
    String metadataJson = "{\"metric_view.type\":\"dimension\",\"metric_view.expr\":\"region\"}";
    View view =
        new View.Builder()
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
            // Spark's `Dependency.table(...)` takes a `String[]` of structural multi-part
            // name parts (was a single dot-joined string before PR #55487 introduced
            // structural deps). The matching mock UC payload below continues to use UC's
            // legacy dot-joined `tableFullName` wire format -- the connector's
            // `toUcDependencyList` joins / `fromUcDependencyList` splits at the boundary.
            .withViewDependencies(
                org.apache.spark.sql.connector.catalog.DependencyList.of(
                    new org.apache.spark.sql.connector.catalog.Dependency[] {
                      org.apache.spark.sql.connector.catalog.Dependency.table(
                          new String[] {"test_catalog", "test_schema", "events"})
                    }))
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
            .viewDefinition(view.queryText())
            .columns(List.of(ucColumn))
            .properties(Map.of("metric_view.from.type", "ASSET"))
            .viewDependencies(ucDeps);
    when(mockTablesApi.createTable(any(CreateTable.class))).thenReturn(ucMetricView);
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    View loaded = proxyViews.createView(Identifier.of(NAMESPACE, "mv1"), view);

    ArgumentCaptor<CreateTable> captor = ArgumentCaptor.forClass(CreateTable.class);
    verify(mockTablesApi).createTable(captor.capture());
    CreateTable request = captor.getValue();
    assertThat(request.getTableType()).isEqualTo(TableType.METRIC_VIEW);
    assertThat(request.getViewDefinition()).isEqualTo(view.queryText());
    assertThat(request.getViewDependencies().getDependencies()).hasSize(1);
    // Wire-side check: `toUcDependencyList` flattened Spark's structural parts back into
    // UC's legacy dot-joined `tableFullName` for the wire payload.
    assertThat(request.getViewDependencies().getDependencies().get(0).getTable().getTableFullName())
        .isEqualTo("test_catalog.test_schema.events");
    assertThat(request.getColumns()).hasSize(1);
    assertThat(request.getColumns().get(0).getTypeJson()).contains("metric_view.type");
    assertThat(loaded.queryText()).isEqualTo(view.queryText());
    assertThat(loaded.viewDependencies().dependencies()).hasSize(1);
    // Spark-side check: `fromUcDependencyList` split the wire dot-joined `tableFullName`
    // back into structural parts. This pins the full Spark structural -> UC wire ->
    // Spark structural round-trip.
    // `dependencies()` and `nameParts()` return Java arrays; AssertJ's `hasSize` /
    // `containsExactly` accept both arrays and `Iterable`, and element access is `[0]`.
    org.apache.spark.sql.connector.catalog.TableDependency loadedDep =
        (org.apache.spark.sql.connector.catalog.TableDependency)
            loaded.viewDependencies().dependencies()[0];
    assertThat(loadedDep.nameParts()).containsExactly("test_catalog", "test_schema", "events");
  }

  @Test
  public void testLoadRelationForMetricViewReturnsView() throws Exception {
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
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    Relation relation = proxyRelations.loadRelation(Identifier.of(NAMESPACE, "mv1"));

    assertThat(relation).isInstanceOf(View.class);
    View view = (View) relation;
    assertThat(view.queryText()).isEqualTo("version: \"0.1\"");
    assertThat(view.properties().get(TableCatalog.PROP_TABLE_TYPE))
        .isEqualTo(TableSummary.METRIC_VIEW_TABLE_TYPE);
  }

  // -- RelationCatalog cross-type filtering --

  @Test
  public void testLoadTableRejectsMetricView() throws Exception {
    // RelationCatalog keeps table and view loads type-safe: loadTable rejects a view row while
    // loadRelation returns it as a View.
    TableInfo ucMetricView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mv1")
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition("version: \"0.1\"");
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    assertThatThrownBy(() -> proxy.loadTable(Identifier.of(NAMESPACE, "mv1")))
        .isInstanceOf(NoSuchTableException.class);
  }

  @Test
  public void testLoadRelationTriggersExactlyOneGetTableRpc() throws Exception {
    // Regression guard for the unified relation API surface.
    TableInfo ucMetricView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mv1")
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition("version: \"0.1\"");
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    proxyRelations.loadRelation(Identifier.of(NAMESPACE, "mv1"));
    verify(mockTablesApi, org.mockito.Mockito.times(1))
        .getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true));
  }

  @Test
  public void testUCSingleCatalogLoadRelationReusesViewFromDelegate() throws Exception {
    TableInfo ucMetricView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mv1")
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition("version: \"0.1\"");
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    Identifier ident = Identifier.of(NAMESPACE, "mv1");
    TableCatalog delegate = org.mockito.Mockito.mock(TableCatalog.class);
    when(delegate.loadTable(eq(ident))).thenAnswer(invocation -> proxy.loadTable(ident));

    UCSingleCatalog catalog = new UCSingleCatalog();
    setCatalogField(catalog, "delegate", delegate);
    setCatalogField(catalog, "ucProxy", proxyRelations);

    Relation relation = ((RelationCatalog) catalog).loadRelation(ident);

    assertThat(relation).isInstanceOf(View.class);
    verify(delegate).loadTable(eq(ident));
    verify(mockTablesApi, org.mockito.Mockito.times(1))
        .getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true));
  }

  @Test
  public void testUCSingleCatalogLoadRelationRethrowsDelegateErrorForRegularTable()
      throws Exception {
    // Safety property (the negative half of the loadRelation try/catch): when the delegate (e.g.
    // Delta) rejects the identifier as a table and the UC row is a *regular* table -- not a view --
    // loadRelation must rethrow the delegate's own NoSuchTableException. It must NOT swallow that
    // error and return the raw UC table, which would silently bypass Delta for a real table.
    TableInfo ucTable =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("t1")
            .tableType(TableType.EXTERNAL);
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.t1"), eq(true), eq(true)))
        .thenReturn(ucTable);

    Identifier ident = Identifier.of(NAMESPACE, "t1");
    NoSuchTableException delegateError = new NoSuchTableException(ident);
    TableCatalog delegate = org.mockito.Mockito.mock(TableCatalog.class);
    when(delegate.loadTable(eq(ident))).thenThrow(delegateError);

    UCSingleCatalog catalog = new UCSingleCatalog();
    setCatalogField(catalog, "delegate", delegate);
    setCatalogField(catalog, "ucProxy", proxyRelations);

    // isSameAs pins that the delegate's exact exception propagates -- not a freshly constructed
    // NoSuchTableException, and not a table returned in its place.
    assertThatThrownBy(() -> ((RelationCatalog) catalog).loadRelation(ident))
        .isSameAs(delegateError);
    verify(delegate).loadTable(eq(ident));
    verify(mockTablesApi, org.mockito.Mockito.never())
        .getTable(eq("test_catalog.test_schema.t1"), eq(true), eq(true));
  }

  @Test
  public void testUCSingleCatalogLoadRelationThrowsNoSuchTableWhenIdentifierMissing()
      throws Exception {
    // When neither the delegate nor UC knows the identifier, loadRelation surfaces the delegate's
    // NoSuchTableException -- not the NoSuchViewException raised internally by the UC fallback.
    Identifier ident = Identifier.of(NAMESPACE, "missing");
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.missing"), eq(true), eq(true)))
        .thenThrow(new ApiException(404, "not found"));

    NoSuchTableException delegateError = new NoSuchTableException(ident);
    TableCatalog delegate = org.mockito.Mockito.mock(TableCatalog.class);
    when(delegate.loadTable(eq(ident))).thenThrow(delegateError);

    UCSingleCatalog catalog = new UCSingleCatalog();
    setCatalogField(catalog, "delegate", delegate);
    setCatalogField(catalog, "ucProxy", proxyRelations);

    assertThatThrownBy(() -> ((RelationCatalog) catalog).loadRelation(ident))
        .isSameAs(delegateError);
    verify(delegate).loadTable(eq(ident));
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
  public void testLoadViewThrowsUnsupportedForListedButUnmappedViewKind() throws Exception {
    // MATERIALIZED_VIEW is listed by listViews (it's view-like) but has no Spark TableSummary
    // mapping yet, so loadView reports it as unsupported -- NOT NoSuchViewException, which would
    // contradict its presence in SHOW VIEWS.
    TableInfo ucMaterializedView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mtv1")
            .tableType(TableType.MATERIALIZED_VIEW);
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mtv1"), eq(true), eq(true)))
        .thenReturn(ucMaterializedView);

    assertThatThrownBy(() -> proxyViews.loadView(Identifier.of(NAMESPACE, "mtv1")))
        .isInstanceOf(UnsupportedOperationException.class);
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
  public void testDropViewReturnsFalseForListedButUnmappedViewKind() throws Exception {
    // A listed-but-unmapped view kind (e.g. MATERIALIZED_VIEW) is treated as uniformly inert:
    // dropView returns false WITHOUT issuing a delete, so a `DROP VIEW IF EXISTS` no-ops rather
    // than the connector attempting to drop a kind it can't otherwise load or create. This is the
    // subtler sibling of testDropViewReturnsFalseForRegularTableAndDoesNotDelete.
    TableInfo ucMaterializedView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mtv1")
            .tableType(TableType.MATERIALIZED_VIEW);
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mtv1"), eq(true), eq(true)))
        .thenReturn(ucMaterializedView);

    boolean dropped = proxyViews.dropView(Identifier.of(NAMESPACE, "mtv1"));

    assertThat(dropped).isFalse();
    verify(mockTablesApi, org.mockito.Mockito.never())
        .deleteTable(org.mockito.ArgumentMatchers.anyString());
  }

  @Test
  public void testLoadViewDegradesToEmptySchemaWhenColumnsNull() throws Exception {
    // `TableInfo.columns` is nullable on the wire (a row written by an older or non-Spark client).
    // toView's `Option(t.getColumns)` guard must degrade to an empty schema rather than NPE-ing
    // out of the view-load path.
    TableInfo ucMetricView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mv1")
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition("version: \"0.1\"")
            .columns(null)
            .properties(Map.of("metric_view.from.type", "ASSET"));
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    View loaded = proxyViews.loadView(Identifier.of(NAMESPACE, "mv1"));

    assertThat(loaded.schema().fields()).isEmpty();
    assertThat(loaded.queryColumnNames()).isEmpty();
  }

  @Test
  public void testReplaceViewThrowsUnsupportedOperation() throws Exception {
    View view =
        new View.Builder()
            .withColumns(new Column[] {Column.create("c", DataTypes.StringType, true)})
            .withProperties(
                Map.of(TableCatalog.PROP_TABLE_TYPE, TableSummary.METRIC_VIEW_TABLE_TYPE))
            .withQueryText("version: \"0.1\"")
            .withCurrentCatalog(CATALOG_NAME)
            .withCurrentNamespace(NAMESPACE)
            .build();

    assertThatThrownBy(() -> proxyViews.replaceView(Identifier.of(NAMESPACE, "mv1"), view))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testRenameViewThrowsUnsupportedOperation() throws Exception {
    assertThatThrownBy(
            () ->
                proxyViews.renameView(
                    Identifier.of(NAMESPACE, "mv1"), Identifier.of(NAMESPACE, "mv2")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testCreateViewDropsFunctionDependencies() throws Exception {
    // UC OSS does not persist function dependencies, so toUcDependencyList silently drops them.
    // Pin that the lossy drop is intentional: a View carrying both a table dep and a function
    // dep must produce a CreateTable payload containing only the table dep.
    View view =
        new View.Builder()
            .withColumns(new Column[] {Column.create("c", DataTypes.StringType, true)})
            .withProperties(
                Map.of(TableCatalog.PROP_TABLE_TYPE, TableSummary.METRIC_VIEW_TABLE_TYPE))
            .withQueryText("version: \"0.1\"")
            .withCurrentCatalog(CATALOG_NAME)
            .withCurrentNamespace(NAMESPACE)
            .withViewDependencies(
                org.apache.spark.sql.connector.catalog.DependencyList.of(
                    new org.apache.spark.sql.connector.catalog.Dependency[] {
                      org.apache.spark.sql.connector.catalog.Dependency.table(
                          new String[] {"test_catalog", "test_schema", "events"}),
                      org.apache.spark.sql.connector.catalog.Dependency.function(
                          new String[] {"test_catalog", "test_schema", "my_udf"})
                    }))
            .build();

    TableInfo ucMetricView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mv1")
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition(view.queryText());
    when(mockTablesApi.createTable(any(CreateTable.class))).thenReturn(ucMetricView);
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    proxyViews.createView(Identifier.of(NAMESPACE, "mv1"), view);

    ArgumentCaptor<CreateTable> captor = ArgumentCaptor.forClass(CreateTable.class);
    verify(mockTablesApi).createTable(captor.capture());
    List<Dependency> sentDeps = captor.getValue().getViewDependencies().getDependencies();
    // Only the table dependency survives; the function dependency was dropped.
    assertThat(sentDeps).hasSize(1);
    assertThat(sentDeps.get(0).getTable().getTableFullName())
        .isEqualTo("test_catalog.test_schema.events");
  }

  @Test
  public void testCreateViewMapsConflictToViewAlreadyExistsException() throws Exception {
    when(mockTablesApi.createTable(any(CreateTable.class)))
        .thenThrow(new ApiException(409, "conflict"));

    View view =
        new View.Builder()
            .withColumns(new Column[] {Column.create("c", DataTypes.StringType, true)})
            .withProperties(
                Map.of(TableCatalog.PROP_TABLE_TYPE, TableSummary.METRIC_VIEW_TABLE_TYPE))
            .withQueryText("version: \"0.1\"")
            .withCurrentCatalog(CATALOG_NAME)
            .withCurrentNamespace(NAMESPACE)
            .build();

    assertThatThrownBy(() -> proxyViews.createView(Identifier.of(NAMESPACE, "mv1"), view))
        .isInstanceOf(ViewAlreadyExistsException.class);
  }

  @Test
  public void testCreateTableMapsConflictToTableAlreadyExistsException() throws Exception {
    when(mockTablesApi.createTable(any(CreateTable.class)))
        .thenThrow(new ApiException(409, "conflict"));

    // The 4-arg createTable method asserts a non-null storage location, so the test must
    // set PROP_LOCATION on the TableInfo for the 409-mapping path to be reached.
    org.apache.spark.sql.connector.catalog.TableInfo sparkInfo =
        new org.apache.spark.sql.connector.catalog.TableInfo.Builder()
            .withColumns(new Column[] {Column.create("c", DataTypes.StringType, true)})
            .withProperties(
                Map.of(
                    TableCatalog.PROP_PROVIDER, "parquet",
                    TableCatalog.PROP_LOCATION, "/tmp/uc-proxy-suite/t1"))
            .build();

    assertThatThrownBy(() -> proxy.createTable(Identifier.of(NAMESPACE, "t1"), sparkInfo))
        .isInstanceOf(TableAlreadyExistsException.class);
  }

  private static void setCatalogField(UCSingleCatalog catalog, String fieldName, Object value) {
    try {
      Field field = UCSingleCatalog.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(catalog, value);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
