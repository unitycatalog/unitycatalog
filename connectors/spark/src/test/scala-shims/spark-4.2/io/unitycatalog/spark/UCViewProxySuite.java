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
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.MetadataTable;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableSummary;
import org.apache.spark.sql.connector.catalog.TableViewCatalog;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.ViewInfo;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Spark-4.2-only view-side unit tests for {@code UCProxy}. Composes the shared
 * {@link UCProxyTestFixture} (which builds the mock-backed proxy) and adds the
 * {@code TableViewCatalog} / {@code ViewCatalog} casts plus the view CRUD / metric-view tests.
 *
 * <p>This file lives under {@code src/test/scala-shims/spark-4.2/}, so the cross-Spark build only
 * compiles and runs it for the Spark 4.2 build, where {@code UCProxy} actually mixes in the V2 view
 * catalog surface ({@code MetadataTable}, {@code ViewInfo}, {@code TableViewCatalog},
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
  private TableViewCatalog proxyRelations;
  private ViewCatalog proxyViews;

  @BeforeEach
  public void setUp() throws Exception {
    UCProxyTestFixture fixture = new UCProxyTestFixture().build();
    mockTablesApi = fixture.mockTablesApi;
    proxy = fixture.proxy;
    // These casts succeed only on the Spark 4.2 build, where UCProxy's UCProxyViewSupport mixes in
    // TableViewCatalog (which extends ViewCatalog). On 4.0/4.1 they would throw, which is why this
    // suite lives in the 4.2-only test source dir.
    proxyRelations = (TableViewCatalog) fixture.proxyObj;
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
            .viewDefinition(viewInfo.queryText())
            .columns(List.of(ucColumn))
            .properties(Map.of("metric_view.from.type", "ASSET"))
            .viewDependencies(ucDeps);
    when(mockTablesApi.createTable(any(CreateTable.class))).thenReturn(ucMetricView);
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    ViewInfo loaded = proxyViews.createView(Identifier.of(NAMESPACE, "mv1"), viewInfo);

    ArgumentCaptor<CreateTable> captor = ArgumentCaptor.forClass(CreateTable.class);
    verify(mockTablesApi).createTable(captor.capture());
    CreateTable request = captor.getValue();
    assertThat(request.getTableType()).isEqualTo(TableType.METRIC_VIEW);
    assertThat(request.getViewDefinition()).isEqualTo(viewInfo.queryText());
    assertThat(request.getViewDependencies().getDependencies()).hasSize(1);
    // Wire-side check: `toUcDependencyList` flattened Spark's structural parts back into
    // UC's legacy dot-joined `tableFullName` for the wire payload.
    assertThat(request.getViewDependencies().getDependencies().get(0).getTable().getTableFullName())
        .isEqualTo("test_catalog.test_schema.events");
    assertThat(request.getColumns()).hasSize(1);
    assertThat(request.getColumns().get(0).getTypeJson()).contains("metric_view.type");
    assertThat(loaded.queryText()).isEqualTo(viewInfo.queryText());
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
  public void testLoadTableOrViewForMetricViewReturnsMetadataTableWithViewInfo() throws Exception {
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

    org.apache.spark.sql.connector.catalog.Table table =
        proxyRelations.loadTableOrView(Identifier.of(NAMESPACE, "mv1"));

    assertThat(table).isInstanceOf(MetadataTable.class);
    ViewInfo view = (ViewInfo) ((MetadataTable) table).getTableInfo();
    assertThat(view.queryText()).isEqualTo("version: \"0.1\"");
    assertThat(view.properties().get(TableCatalog.PROP_TABLE_TYPE))
        .isEqualTo(TableSummary.METRIC_VIEW_TABLE_TYPE);
    // TableViewCatalog spec recommends MetadataTable.name() be the v2 ident.toString()
    // (quoted multi-part form, no catalog prefix) so DESCRIBE TABLE EXTENDED's `Name` row
    // renders the identifier consistently with every other catalog.
    assertThat(table.name()).isEqualTo(Identifier.of(NAMESPACE, "mv1").toString());
  }

  // -- TableViewCatalog cross-type filtering --

  @Test
  public void testLoadTableReturnsMetadataTableForMetricView() throws Exception {
    // UCProxy.loadTable is intentionally permissive (returns MetadataTable+ViewInfo for
    // view-like rows) so that when Delta's `super.loadTable` lands here through
    // `DelegatingCatalogExtension`, the view shape can flow back unchanged via Delta's
    // `case o => o` fallthrough. The contract that "loadTable rejects views" is enforced
    // one layer up at `UCSingleCatalog.loadTable`, NOT here -- see the comment block on
    // `UCSingleCatalog.loadTable` for why.
    TableInfo ucMetricView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mv1")
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition("version: \"0.1\"");
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    org.apache.spark.sql.connector.catalog.Table table =
        proxy.loadTable(Identifier.of(NAMESPACE, "mv1"));

    assertThat(table).isInstanceOf(MetadataTable.class);
    assertThat(((MetadataTable) table).getTableInfo()).isInstanceOf(ViewInfo.class);
  }

  @Test
  public void testLoadTableOrViewTriggersExactlyOneGetTableRpc() throws Exception {
    // Same regression guard but exercised through the `loadTableOrView` API surface.
    // After the flip `loadTableOrView` is a thin passthrough to `loadTable`, so it must
    // also stay at one RPC per call.
    TableInfo ucMetricView =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("mv1")
            .tableType(TableType.METRIC_VIEW)
            .viewDefinition("version: \"0.1\"");
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true)))
        .thenReturn(ucMetricView);

    proxyRelations.loadTableOrView(Identifier.of(NAMESPACE, "mv1"));
    verify(mockTablesApi, org.mockito.Mockito.times(1))
        .getTable(eq("test_catalog.test_schema.mv1"), eq(true), eq(true));
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
}
