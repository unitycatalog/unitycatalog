package io.unitycatalog.spark;

import static io.unitycatalog.spark.UCProxyTestFixture.CATALOG_NAME;
import static io.unitycatalog.spark.UCProxyTestFixture.NAMESPACE;
import static io.unitycatalog.spark.UCProxyTestFixture.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Spark-4.0/4.1 view-unsupported tests -- the pre-view-API counterpart to the Spark-4.2
 * {@code UCViewProxySuite} (resolved per Spark version via the {@code scala-shims/} test dirs).
 * Spark 4.0/4.1 have no view-catalog APIs ({@code RelationCatalog}, {@code ViewCatalog},
 * {@code View}) -- the connector's {@code UCProxyViewSupport} shim is a no-op.
 *
 * <p>So on Spark 4.0/4.1 there is nothing like {@code createView}/{@code loadView} to call; this
 * suite pins that view-like UC rows ({@code METRIC_VIEW} and {@code MATERIALIZED_VIEW}) are inert
 * through every {@code TableCatalog} operation, and that the generic table operations offer no
 * back-door to view creation. On Spark 4.2 the mapped {@code METRIC_VIEW} behaves differently (see
 * the 4.2 {@code UCViewProxySuite}).
 */
public class UCViewUnsupportedSuite {

  private TablesApi mockTablesApi;
  private TableCatalog proxy;

  @BeforeEach
  public void setUp() throws Exception {
    UCProxyTestFixture fixture = new UCProxyTestFixture().build();
    mockTablesApi = fixture.mockTablesApi;
    proxy = fixture.proxy;
  }

  private void stubGetTable(String name, TableType tableType) throws Exception {
    TableInfo row =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name(name)
            .tableType(tableType)
            .viewDefinition("version: \"0.1\"");
    when(mockTablesApi.getTable(eq("test_catalog.test_schema." + name), eq(true), eq(true)))
        .thenReturn(row);
  }

  // -- loadTable: view-like rows cannot be materialized as tables --

  @Test
  public void testLoadTableRejectsMetricView() throws Exception {
    stubGetTable("mv1", TableType.METRIC_VIEW);
    assertThatThrownBy(() -> proxy.loadTable(Identifier.of(NAMESPACE, "mv1")))
        .isInstanceOf(NoSuchTableException.class);
  }

  @Test
  public void testLoadTableRejectsMaterializedView() throws Exception {
    stubGetTable("mtv1", TableType.MATERIALIZED_VIEW);
    assertThatThrownBy(() -> proxy.loadTable(Identifier.of(NAMESPACE, "mtv1")))
        .isInstanceOf(NoSuchTableException.class);
  }

  // -- listTables: view-like rows are hidden from the table surface --

  @Test
  public void testListTablesHidesViewLikeRows() throws Exception {
    ListTablesResponse response =
        new ListTablesResponse()
            .tables(
                List.of(
                    new TableInfo().name("t1").tableType(TableType.EXTERNAL),
                    new TableInfo().name("mv1").tableType(TableType.METRIC_VIEW),
                    new TableInfo().name("mtv1").tableType(TableType.MATERIALIZED_VIEW)))
            .nextPageToken(null);
    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull()))
        .thenReturn(response);

    assertThat(proxy.listTables(NAMESPACE)).containsExactly(Identifier.of(NAMESPACE, "t1"));
  }

  // -- dropTable: inert for view-like rows (returns false, no delete) --

  @Test
  public void testDropTableIsInertForMetricView() throws Exception {
    stubGetTable("mv1", TableType.METRIC_VIEW);
    assertThat(proxy.dropTable(Identifier.of(NAMESPACE, "mv1"))).isFalse();
    verify(mockTablesApi, never()).deleteTable(anyString());
  }

  @Test
  public void testDropTableIsInertForMaterializedView() throws Exception {
    stubGetTable("mtv1", TableType.MATERIALIZED_VIEW);
    assertThat(proxy.dropTable(Identifier.of(NAMESPACE, "mtv1"))).isFalse();
    verify(mockTablesApi, never()).deleteTable(anyString());
  }

  // -- createTable: no view-creation back-door via the table surface --

  @Test
  public void testCreateTableIgnoresViewTableTypeAndStillRequiresProvider() {
    // A `table_type=METRIC_VIEW` property does not route to any view-create path on Spark 4.0/4.1;
    // createTable treats it as a normal table and still enforces the provider requirement. No
    // metric view is ever created, and no createTable RPC is issued.
    // Use the connector's own constant, which compiles on all versions: Spark's
    // TableCatalog.PROP_TABLE_TYPE was only added in Spark 4.1, so it does not exist on Spark 4.0.
    Map<String, String> props = Map.of(UCTableProperties.PROP_TABLE_TYPE, "METRIC_VIEW");
    assertThatThrownBy(
            () ->
                proxy.createTable(
                    Identifier.of(NAMESPACE, "mv_attempt"),
                    new Column[] {Column.create("c", DataTypes.StringType, true)},
                    new Transform[0],
                    props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // -- alterTable / renameTable: unsupported for any table (no view escape hatch) --

  @Test
  public void testAlterTableUnsupported() {
    assertThatThrownBy(
            () -> proxy.alterTable(Identifier.of(NAMESPACE, "mv1"), new TableChange[0]))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testRenameTableUnsupported() {
    assertThatThrownBy(
            () ->
                proxy.renameTable(
                    Identifier.of(NAMESPACE, "mv1"), Identifier.of(NAMESPACE, "mv2")))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
