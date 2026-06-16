package io.unitycatalog.spark;

import static io.unitycatalog.spark.UCProxyTestFixture.CATALOG_NAME;
import static io.unitycatalog.spark.UCProxyTestFixture.NAMESPACE;
import static io.unitycatalog.spark.UCProxyTestFixture.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.ListSchemasResponse;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Version-agnostic unit tests for {@code UCProxy} (the table/namespace surface that exists on every
 * supported Spark version). Lives under {@code src/test/java/}, so it compiles and runs against
 * Spark 4.0, 4.1, and 4.2. It composes {@link UCProxyTestFixture} (rather than inheriting it) and
 * references no Spark-4.2-only type. The view-side tests live in {@code UCViewProxySuite} under
 * {@code src/test/scala-shims/spark-4.2/}.
 *
 * <p>End-to-end integration tests that exercise pagination through UCSingleCatalog and a real UC
 * server are in {@link BaseTableReadWriteTest#testListTablesPagination} and {@link
 * SchemaOperationsTest#testListSchemasPagination}.
 */
public class UCProxySuite {

  private TablesApi mockTablesApi;
  private SchemasApi mockSchemasApi;
  private TableCatalog proxy;
  private SupportsNamespaces proxyNs;

  @BeforeEach
  public void setUp() throws Exception {
    UCProxyTestFixture fixture = new UCProxyTestFixture().build();
    mockTablesApi = fixture.mockTablesApi;
    mockSchemasApi = fixture.mockSchemasApi;
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
    // flip, `UCSingleCatalog.loadTableOrView` would call `getUcTable` for view
    // classification, then re-fetch via `delegate.loadTable -> UCProxy.loadTableOrView ->
    // getUcTable`, producing two `tablesApi.getTable` RPCs per identifier resolution. After
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

    // `loadTable` issues the single `getTable` RPC first (via `getUcTable`), then proceeds to
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
}
