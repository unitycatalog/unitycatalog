package io.unitycatalog.spark;

import static io.unitycatalog.spark.UCProxyTestFixture.CATALOG_NAME;
import static io.unitycatalog.spark.UCProxyTestFixture.NAMESPACE;
import static io.unitycatalog.spark.UCProxyTestFixture.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaRenameTableRequest;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ListSchemasResponse;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Version-agnostic unit tests for {@code UCProxy} (the table/namespace surface that exists on every
 * supported Spark version). Lives under {@code src/test/java/}, so it compiles and runs against
 * Spark 4.0, 4.1, and 4.2. It composes {@link UCProxyTestFixture} (rather than inheriting it) and
 * references no Spark-4.2-only type. The view-side tests live in {@code UCViewProxySuite} under
 * {@code src/test/scala-shims/spark-4.2-4.3/}; the pre-view-API (4.0/4.1) unsupported-path tests
 * live in {@code UCViewUnsupportedSuite} under {@code src/test/scala-shims/spark-4.0-4.1/}.
 *
 * <p>End-to-end integration tests that exercise pagination through UCSingleCatalog and a real UC
 * server are in {@link BaseTableReadWriteTest#testListTablesPagination} and {@link
 * SchemaOperationsTest#testListSchemasPagination}.
 */
public class UCProxySuite {

  private TablesApi mockTablesApi;
  private SchemasApi mockSchemasApi;
  private DeltaTablesApi mockDeltaTablesApi;
  private TableCatalog proxy;
  private SupportsNamespaces proxyNs;

  @BeforeEach
  public void setUp() throws Exception {
    UCProxyTestFixture fixture = new UCProxyTestFixture().build();
    mockTablesApi = fixture.mockTablesApi;
    mockSchemasApi = fixture.mockSchemasApi;
    mockDeltaTablesApi = fixture.mockDeltaTablesApi;
    proxy = fixture.proxy;
    proxyNs = fixture.proxyNs;
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

  // -- loadTable (table surface) tests --

  @Test
  public void testLoadTableTriggersExactlyOneGetTableRpc() throws Exception {
    // Regression guard for the double-RPC bug fixed in
    // https://github.com/unitycatalog/unitycatalog/pull/1513 review feedback. Before the
    // flip, `UCSingleCatalog.loadTableOrView` would call `getUCTableLike` for view
    // classification, then re-fetch via `delegate.loadTable -> UCProxy.loadTableOrView ->
    // getUCTableLike`, producing two `tablesApi.getTable` RPCs per identifier resolution. After
    // the flip, `UCProxy.loadTable` is the single primitive and is called exactly once per
    // resolution.
    TableInfo ucTable =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("t1")
            .tableId("test-table-id")
            .tableType(TableType.EXTERNAL)
            .storageLocation("file:///tmp/t1")
            .dataSourceFormat(io.unitycatalog.client.model.DataSourceFormat.PARQUET)
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("id")
                        .typeName(ColumnTypeName.INT)
                        .typeText("int")
                        .typeJson(
                            "{\"name\":\"id\",\"type\":\"integer\","
                                + "\"nullable\":true,\"metadata\":{}}")
                        .nullable(true)
                        .position(0)));
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.t1"), eq(true), eq(true)))
        .thenReturn(ucTable);

    // `loadTable` issues the single `getTable` RPC first (via `getUCTableLike`), then proceeds to
    // `loadV1Table`, which vends credentials through the mock `apiClient` and may throw. The
    // regression guard is purely about the RPC count, so swallow any downstream failure and
    // assert the count -- this keeps the test hermetic and identical across Spark versions.
    try {
      proxy.loadTable(Identifier.of(NAMESPACE, "t1"));
    } catch (RuntimeException credentialVendingFailure) {
      // Expected in the mock context: credential vending has no real UC endpoint.
    }
    verify(mockTablesApi, org.mockito.Mockito.times(1))
        .getTable(eq("test_catalog.test_schema.t1"), eq(true), eq(true));
  }

  /**
   * Spark asks the current catalog to load every relation before falling back to path-based
   * resolution, so a bare {@code parquet.`s3://.../file.parquet`} arrives at {@code loadTable} as
   * namespace {@code parquet} with the path as the table name. UC addresses a table by the dotted
   * string {@code catalog.schema.table} and would 400 if asked; {@code CatalogV2Util.loadTable}
   * only absorbs a not-found answer. A name UC cannot address can never denote a table UC holds, so
   * this returns {@link NoSuchTableException} without an RPC (servers disagree 400 vs 404 on the
   * same bad name).
   */
  @Test
  public void testLoadTableSkipsRpcForNameUcCannotAddress() throws Exception {
    String path = "s3://bucket/tenants/t1/tmp-ingest-4825.parquet";

    assertThatThrownBy(() -> proxy.loadTable(Identifier.of(new String[] {"parquet"}, path)))
        .isInstanceOf(NoSuchTableException.class);

    verify(mockTablesApi, org.mockito.Mockito.never())
        .getTable(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any());
  }

  /** Extensionless cloud paths also carry `/` and must skip the UC RPC. */
  @Test
  public void testLoadTableSkipsRpcForExtensionlessBarePath() throws Exception {
    String path = "s3://bucket/dir/part";

    assertThatThrownBy(() -> proxy.loadTable(Identifier.of(new String[] {"parquet"}, path)))
        .isInstanceOf(NoSuchTableException.class);

    verify(mockTablesApi, org.mockito.Mockito.never())
        .getTable(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any());
  }

  // -- dropTable (table surface) tests --

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

  // -- renameTable tests --

  private static final Identifier RENAME_SOURCE = Identifier.of(NAMESPACE, "src");
  private static final Identifier RENAME_TARGET = Identifier.of(NAMESPACE, "dst");

  private void stubRenameSource(TableType tableType, DataSourceFormat format) throws Exception {
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.src"), eq(false), eq(false)))
        .thenReturn(
            new TableInfo()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .name("src")
                .tableType(tableType)
                .dataSourceFormat(format));
  }

  private void stubRenameFailure(ApiException e) throws Exception {
    doThrow(e).when(mockDeltaTablesApi).renameTable(anyString(), anyString(), anyString(), any());
  }

  private void verifyRenameCalled(String newName) throws Exception {
    verify(mockDeltaTablesApi)
        .renameTable(
            eq(CATALOG_NAME),
            eq(SCHEMA_NAME),
            eq("src"),
            eq(new DeltaRenameTableRequest().newName(newName)));
  }

  private void verifyRenameNotCalled() throws Exception {
    verify(mockDeltaTablesApi, never()).renameTable(anyString(), anyString(), anyString(), any());
  }

  private static ApiException deltaError(int code, String type) {
    return new ApiException(
        code,
        "renameTable call failed",
        null,
        "{\"error\":{\"code\":" + code + ",\"type\":\"" + type + "\",\"message\":\"m\"}}");
  }

  @ParameterizedTest
  @EnumSource(
      value = TableType.class,
      names = {"MANAGED", "EXTERNAL"})
  public void testRenameTableCallsDeltaRenameForDeltaTables(TableType tableType) throws Exception {
    stubRenameSource(tableType, DataSourceFormat.DELTA);
    proxy.renameTable(RENAME_SOURCE, RENAME_TARGET);
    verifyRenameCalled("dst");
  }

  // Each namespace is wrapped in Arguments: a bare String[] would be spread into one argument per
  // element.
  static Stream<Arguments> sameSchemaRenameTargetNamespaces() {
    return Stream.of(
            new String[0], new String[] {SCHEMA_NAME}, new String[] {CATALOG_NAME, SCHEMA_NAME})
        .map(ns -> Arguments.of((Object) ns));
  }

  @ParameterizedTest
  @MethodSource("sameSchemaRenameTargetNamespaces")
  public void testRenameTableAcceptsTargetInSameSchema(String[] namespace) throws Exception {
    stubRenameSource(TableType.MANAGED, DataSourceFormat.DELTA);
    proxy.renameTable(RENAME_SOURCE, Identifier.of(namespace, "dst"));
    verifyRenameCalled("dst");
  }

  static Stream<Arguments> otherSchemaRenameTargetNamespaces() {
    return Stream.of(
            new String[] {"other_schema"},
            new String[] {"other_catalog", SCHEMA_NAME},
            new String[] {CATALOG_NAME, "other_schema"},
            new String[] {CATALOG_NAME, SCHEMA_NAME, "x"})
        .map(ns -> Arguments.of((Object) ns));
  }

  @ParameterizedTest
  @MethodSource("otherSchemaRenameTargetNamespaces")
  public void testRenameTableRejectsTargetInOtherCatalogOrSchema(String[] namespace)
      throws Exception {
    assertThatThrownBy(() -> proxy.renameTable(RENAME_SOURCE, Identifier.of(namespace, "dst")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("another catalog or schema");
    // Rejected before any RPC.
    verify(mockTablesApi, never()).getTable(anyString(), anyBoolean(), anyBoolean());
    verifyRenameNotCalled();
  }

  @Test
  public void testRenameTableMissingSourceThrowsNoSuchTable() throws Exception {
    when(mockTablesApi.getTable(eq("test_catalog.test_schema.src"), eq(false), eq(false)))
        .thenThrow(new ApiException(404, "not found"));
    assertThatThrownBy(() -> proxy.renameTable(RENAME_SOURCE, RENAME_TARGET))
        .isInstanceOf(NoSuchTableException.class);
    verifyRenameNotCalled();
  }

  @ParameterizedTest
  @EnumSource(
      value = TableType.class,
      names = {"VIEW", "METRIC_VIEW", "MATERIALIZED_VIEW"})
  public void testRenameTableRejectsViews(TableType viewType) throws Exception {
    stubRenameSource(viewType, null);
    assertThatThrownBy(() -> proxy.renameTable(RENAME_SOURCE, RENAME_TARGET))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("has type " + viewType.getValue() + ", not a table");
    verifyRenameNotCalled();
  }

  static Stream<Arguments> nonDeltaRenameSources() {
    return Stream.of(
        Arguments.of(TableType.EXTERNAL, DataSourceFormat.ICEBERG),
        Arguments.of(TableType.EXTERNAL, DataSourceFormat.PARQUET),
        Arguments.of(TableType.EXTERNAL, DataSourceFormat.CSV),
        // A Delta-format streaming table is not a plain table either.
        Arguments.of(TableType.STREAMING_TABLE, DataSourceFormat.DELTA));
  }

  @ParameterizedTest
  @MethodSource("nonDeltaRenameSources")
  public void testRenameTableRejectsNonDeltaTables(TableType tableType, DataSourceFormat format)
      throws Exception {
    stubRenameSource(tableType, format);
    assertThatThrownBy(() -> proxy.renameTable(RENAME_SOURCE, RENAME_TARGET))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("only supported for Delta tables")
        .hasMessageContaining(
            "has type " + tableType.getValue() + " and format " + format.getValue());
    verifyRenameNotCalled();
  }

  static Stream<Arguments> mappedRenameErrors() {
    return Stream.of(
        Arguments.of(
            deltaError(409, "AlreadyExistsException"), TableAlreadyExistsException.class, ""),
        // The source was dropped between the lookup and the rename.
        Arguments.of(deltaError(404, "NoSuchTableException"), NoSuchTableException.class, ""),
        Arguments.of(
            deltaError(400, "UnsupportedTableFormatException"),
            UnsupportedOperationException.class,
            "only supported for Delta tables"),
        // A 404 with no Delta error body: the server has no rename route.
        Arguments.of(
            new ApiException(404, "renameTable call failed", null, "Not Found"),
            UnsupportedOperationException.class,
            "not supported by this Unity Catalog server"));
  }

  @ParameterizedTest
  @MethodSource("mappedRenameErrors")
  public void testRenameTableMapsServerErrors(
      ApiException serverError, Class<? extends Throwable> expectedType, String expectedMessage)
      throws Exception {
    stubRenameSource(TableType.MANAGED, DataSourceFormat.DELTA);
    stubRenameFailure(serverError);
    assertThatThrownBy(() -> proxy.renameTable(RENAME_SOURCE, RENAME_TARGET))
        .isInstanceOf(expectedType)
        .hasMessageContaining(expectedMessage);
  }

  @Test
  public void testRenameTablePassesThroughOtherServerErrors() throws Exception {
    stubRenameSource(TableType.MANAGED, DataSourceFormat.DELTA);
    ApiException denied = deltaError(403, "PermissionDeniedException");
    stubRenameFailure(denied);
    assertThatThrownBy(() -> proxy.renameTable(RENAME_SOURCE, RENAME_TARGET)).isSameAs(denied);
  }
}
