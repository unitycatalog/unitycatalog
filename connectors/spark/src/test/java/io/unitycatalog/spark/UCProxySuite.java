package io.unitycatalog.spark;

import static io.unitycatalog.spark.UCProxyTestFixture.CATALOG_NAME;
import static io.unitycatalog.spark.UCProxyTestFixture.NAMESPACE;
import static io.unitycatalog.spark.UCProxyTestFixture.SCHEMA_NAME;
import static io.unitycatalog.spark.UCProxyTestFixture.routeToProxy;
import static io.unitycatalog.spark.UCProxyTestFixture.sessionWithCatalog;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ListSchemasResponse;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

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

  private static final Map<String, String> PARQUET_AT_LOCATION =
      Map.of(TableCatalog.PROP_PROVIDER, "parquet", TableCatalog.PROP_LOCATION, "/tmp/uc-proxy/t1");

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
    when(mockTablesApi.listTables(
            eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull(), /* includeBrowse= */ eq(false)))
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

    when(mockTablesApi.listTables(
            eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull(), /* includeBrowse= */ eq(false)))
        .thenReturn(page1);
    when(mockTablesApi.listTables(
            eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), eq("token1"), /* includeBrowse= */ eq(false)))
        .thenReturn(page2);

    Identifier[] result = proxy.listTables(NAMESPACE);

    assertThat(result).hasSize(3);
    assertThat(result[0]).isEqualTo(Identifier.of(NAMESPACE, "table1"));
    assertThat(result[1]).isEqualTo(Identifier.of(NAMESPACE, "table2"));
    assertThat(result[2]).isEqualTo(Identifier.of(NAMESPACE, "table3"));

    verify(mockTablesApi)
        .listTables(
            eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull(), /* includeBrowse= */ eq(false));
    verify(mockTablesApi)
        .listTables(
            eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), eq("token1"), /* includeBrowse= */ eq(false));
  }

  @Test
  public void testListTablesEmptyResult() throws Exception {
    ListTablesResponse response =
        new ListTablesResponse().tables(Collections.emptyList()).nextPageToken(null);
    when(mockTablesApi.listTables(
            eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull(), /* includeBrowse= */ eq(false)))
        .thenReturn(response);

    Identifier[] result = proxy.listTables(NAMESPACE);

    assertThat(result).isEmpty();
  }

  @Test
  public void testListTablesEmptyStringPageTokenStopsPagination() throws Exception {
    ListTablesResponse response =
        new ListTablesResponse().tables(List.of(new TableInfo().name("table1"))).nextPageToken("");
    when(mockTablesApi.listTables(
            eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull(), /* includeBrowse= */ eq(false)))
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
    when(mockTablesApi.listTables(
            eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull(), /* includeBrowse= */ eq(false)))
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
    when(mockTablesApi.getTable(
            eq("test_catalog.test_schema.t1"), eq(true), eq(true), /* includeBrowse= */ eq(false)))
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
        .getTable(
            eq("test_catalog.test_schema.t1"), eq(true), eq(true), /* includeBrowse= */ eq(false));
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
            org.mockito.ArgumentMatchers.any(),
            /* includeBrowse= */ org.mockito.ArgumentMatchers.any());
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
            org.mockito.ArgumentMatchers.any(),
            /* includeBrowse= */ org.mockito.ArgumentMatchers.any());
  }

  private Table loadPartitionedTable(DataSourceFormat format) throws Exception {
    TableInfo ucTable =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("t_parts")
            .tableId("table-id-t_parts")
            .tableType(TableType.EXTERNAL)
            .storageLocation("file:/tmp/uc/t_parts")
            .dataSourceFormat(format)
            .columns(
                List.of(
                    intColumn("a", 0, null),
                    intColumn("p", 1, null).partitionIndex(1).comment("p note"),
                    intColumn("b", 2, null),
                    intColumn(
                            "q",
                            3,
                            "{\"name\":\"q\",\"type\":\"integer\",\"nullable\":true,"
                                + "\"metadata\":{\"CURRENT_DEFAULT\":\"7\"}}")
                        .partitionIndex(0)));
    when(mockTablesApi.getTable(
            eq("test_catalog.test_schema.t_parts"),
            eq(true),
            eq(true),
            /* includeBrowse= */ eq(false)))
        .thenReturn(ucTable);
    return proxy.loadTable(Identifier.of(NAMESPACE, "t_parts"));
  }

  private static String[] partitionColumnNames(Table table) {
    return Arrays.stream(table.partitioning())
        .map(transform -> transform.references()[0].fieldNames()[0])
        .toArray(String[]::new);
  }

  @ParameterizedTest
  @EnumSource(
      value = DataSourceFormat.class,
      names = {"PARQUET", "ORC", "JSON", "CSV", "AVRO", "TEXT"})
  public void testLoadFileSourceTablePutsPartitionColumnsLast(DataSourceFormat format)
      throws Exception {
    Table table = loadPartitionedTable(format);

    StructType schema = table.schema();
    assertThat(schema.fieldNames()).containsExactly("a", "b", "q", "p");
    assertThat(partitionColumnNames(table)).containsExactly("q", "p");
    assertThat(schema.apply("p").getComment().get()).isEqualTo("p note");
    assertThat(schema.apply("q").metadata().getString("CURRENT_DEFAULT")).isEqualTo("7");
  }

  @ParameterizedTest
  @EnumSource(
      value = DataSourceFormat.class,
      names = {"DELTA", "ICEBERG"})
  public void testLoadNonFileSourceTableKeepsUCColumnOrder(DataSourceFormat format)
      throws Exception {
    Table table = loadPartitionedTable(format);

    assertThat(table.schema().fieldNames()).containsExactly("a", "p", "b", "q");
    assertThat(partitionColumnNames(table)).containsExactly("q", "p");
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
    when(mockTablesApi.getTable(
            eq("test_catalog.test_schema.mv1"), eq(true), eq(true), /* includeBrowse= */ eq(false)))
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
    when(mockSchemasApi.listSchemas(
            eq(CATALOG_NAME), eq(0), isNull(), /* includeBrowse= */ eq(false)))
        .thenReturn(response);

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

    when(mockSchemasApi.listSchemas(
            eq(CATALOG_NAME), eq(0), isNull(), /* includeBrowse= */ eq(false)))
        .thenReturn(page1);
    when(mockSchemasApi.listSchemas(
            eq(CATALOG_NAME), eq(0), eq("token1"), /* includeBrowse= */ eq(false)))
        .thenReturn(page2);

    String[][] result = proxyNs.listNamespaces();

    assertThat(result).hasNumberOfRows(3);
    assertThat(result[0]).containsExactly("schema1");
    assertThat(result[1]).containsExactly("schema2");
    assertThat(result[2]).containsExactly("schema3");

    verify(mockSchemasApi)
        .listSchemas(eq(CATALOG_NAME), eq(0), isNull(), /* includeBrowse= */ eq(false));
    verify(mockSchemasApi)
        .listSchemas(eq(CATALOG_NAME), eq(0), eq("token1"), /* includeBrowse= */ eq(false));
  }

  @Test
  public void testListNamespacesEmptyStringPageTokenStopsPagination() throws Exception {
    ListSchemasResponse response =
        new ListSchemasResponse()
            .schemas(List.of(new SchemaInfo().name("schema1")))
            .nextPageToken("");
    when(mockSchemasApi.listSchemas(
            eq(CATALOG_NAME), eq(0), isNull(), /* includeBrowse= */ eq(false)))
        .thenReturn(response);

    String[][] result = proxyNs.listNamespaces();

    assertThat(result).hasNumberOfRows(1);
    assertThat(result[0]).containsExactly("schema1");
  }

  // -- toStructField: ColumnInfo.comment wins over a comment stored in type_json metadata --

  private static ColumnInfo columnWithStoredComment(String ucComment) {
    return new ColumnInfo()
        .name("c")
        .typeText("int")
        .nullable(true)
        .comment(ucComment)
        .typeJson(
            "{\"name\":\"c\",\"type\":\"integer\",\"nullable\":true,"
                + "\"metadata\":{\"comment\":\"stored\",\"k\":\"v\"}}");
  }

  @Test
  public void testToStructFieldPrefersColumnInfoComment() {
    StructField field = ((UCProxy) proxy).toStructField(columnWithStoredComment("uc"));
    assertThat(field.getComment().get()).isEqualTo("uc");
    assertThat(field.metadata().getString("k")).isEqualTo("v");
  }

  @Test
  public void testToStructFieldNullColumnInfoCommentDropsStoredComment() {
    StructField field = ((UCProxy) proxy).toStructField(columnWithStoredComment(null));
    assertThat(field.getComment().isEmpty()).isTrue();
    assertThat(field.metadata().getString("k")).isEqualTo("v");
  }

  private static ColumnInfo intColumn(String name, int position, String typeJson) {
    return new ColumnInfo()
        .name(name)
        .typeName(ColumnTypeName.INT)
        .typeText("int")
        .typeJson(typeJson)
        .nullable(true)
        .position(position);
  }

  @Test
  public void testLoadTableWithUnreadableTypeJsonKeepsOtherColumnsMetadata() throws Exception {
    TableInfo ucTable =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("t_bad_json")
            .tableId("table-id-t_bad_json")
            .tableType(TableType.EXTERNAL)
            .storageLocation("file:/tmp/uc/t_bad_json")
            .dataSourceFormat(DataSourceFormat.PARQUET)
            .columns(
                List.of(
                    intColumn("malformed", 0, "{not json"),
                    intColumn(
                        "unrepresentable",
                        1,
                        "{\"name\":\"unrepresentable\",\"type\":\"integer\",\"nullable\":true,"
                            + "\"metadata\":{\"mixed\":[1,\"a\"]}}"),
                    intColumn(
                        "defaulted",
                        2,
                        "{\"name\":\"defaulted\",\"type\":\"integer\",\"nullable\":true,"
                            + "\"metadata\":{\"CURRENT_DEFAULT\":\"42\","
                            + "\"EXISTS_DEFAULT\":\"42\"}}")));
    when(mockTablesApi.getTable(
            eq("test_catalog.test_schema.t_bad_json"),
            eq(true),
            eq(true),
            /* includeBrowse= */ eq(false)))
        .thenReturn(ucTable);

    StructType schema = proxy.loadTable(Identifier.of(NAMESPACE, "t_bad_json")).schema();

    Metadata nullComment = new MetadataBuilder().putNull("comment").build();
    Metadata defaultMetadata =
        new MetadataBuilder()
            .withMetadata(nullComment)
            .putString("CURRENT_DEFAULT", "42")
            .putString("EXISTS_DEFAULT", "42")
            .build();
    assertThat(schema)
        .isEqualTo(
            new StructType()
                .add("malformed", DataTypes.IntegerType, true, nullComment)
                .add("unrepresentable", DataTypes.IntegerType, true, nullComment)
                .add("defaulted", DataTypes.IntegerType, true, defaultMetadata));
  }

  private CatalogTable loadCatalogTable(
      String name, DataSourceFormat format, Map<String, String> props) throws Exception {
    TableInfo ucTable =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name(name)
            .tableId("table-id-" + name)
            .tableType(TableType.EXTERNAL)
            .storageLocation("file:/tmp/uc/" + name)
            .dataSourceFormat(format)
            .comment("uc comment")
            .properties(props)
            .columns(
                List.of(
                    intColumn(
                        "i",
                        0,
                        "{\"name\":\"i\",\"type\":\"integer\","
                            + "\"nullable\":true,\"metadata\":{}}")));
    when(mockTablesApi.getTable(
            eq("test_catalog.test_schema." + name), eq(true), eq(true), eq(false)))
        .thenReturn(ucTable);
    return ((V1Table) proxy.loadTable(Identifier.of(NAMESPACE, name))).catalogTable();
  }

  @Test
  public void testLoadTableTurnsOptionPrefixedPropertiesIntoOptions() throws Exception {
    CatalogTable table =
        loadCatalogTable(
            "t_options", DataSourceFormat.CSV, Map.of("option.header", "true", "k", "v"));

    assertThat(CollectionConverters.asJava(table.storage().properties()))
        .containsExactlyInAnyOrderEntriesOf(Map.of("header", "true"));
    assertThat(CollectionConverters.asJava(table.properties()))
        .containsExactlyInAnyOrderEntriesOf(Map.of("k", "v"));
    assertThat(table.comment()).isEqualTo(Option.apply("uc comment"));
  }

  @Test
  public void testLoadTableDropsOnlyBareKeysThatDuplicateTheirOption() throws Exception {
    CatalogTable table =
        loadCatalogTable(
            "t_collision",
            DataSourceFormat.CSV,
            Map.of(
                "option.header", "true",
                "header", "true",
                "option.sep", "|",
                "sep", ",",
                "option.Mode", "FAILFAST",
                "mode", "FAILFAST"));

    assertThat(CollectionConverters.asJava(table.storage().properties()))
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("header", "true", "sep", "|", "Mode", "FAILFAST"));
    assertThat(CollectionConverters.asJava(table.properties()))
        .containsExactlyInAnyOrderEntriesOf(Map.of("sep", ",", "mode", "FAILFAST"));
  }

  // -- create conflict tests --

  /**
   * UC REST rejects a taken table name with 400 TABLE_ALREADY_EXISTS, for example when another
   * writer creates the table after Spark's existence check, or when a view holds the name. CREATE
   * TABLE IF NOT EXISTS must then do nothing, and a plain CREATE TABLE must fail as already exists.
   */
  @Test
  public void testCreateTableOnTakenNameHonorsIfNotExists(@TempDir Path dir) throws Exception {
    when(mockTablesApi.getTable(anyString(), anyBoolean(), anyBoolean(), anyBoolean()))
        .thenThrow(new ApiException(404, "not found"));
    when(mockTablesApi.createTable(any()))
        .thenThrow(
            badRequest(
                "{\"error_code\":\"TABLE_ALREADY_EXISTS\",\"message\":\"Table already exists:"
                    + " test_catalog.test_schema.t1\"}"));
    SparkSession spark = sessionWithCatalog(CATALOG_NAME);
    try {
      routeToProxy(spark, CATALOG_NAME, (UCProxy) proxy);
      String createSql =
          "CREATE TABLE %s test_catalog.test_schema.t1 (c STRING) USING PARQUET LOCATION '"
              + dir.toUri()
              + "'";

      spark.sql(String.format(createSql, "IF NOT EXISTS"));
      assertThatThrownBy(() -> spark.sql(String.format(createSql, "")))
          .isInstanceOf(TableAlreadyExistsException.class);
      verify(mockTablesApi, times(2)).createTable(any());
    } finally {
      spark.stop();
    }
  }

  @Test
  public void testCreateTableKeepsOtherBadRequestErrors() throws Exception {
    Identifier ident = Identifier.of(NAMESPACE, "t1");
    Column[] columns = {Column.create("c", DataTypes.StringType)};
    for (String body :
        Arrays.asList(
            null,
            "",
            "Bad Request",
            "{\"error_code\":\"INVALID_ARGUMENT\",\"message\":\"Invalid name:"
                + " TABLE_ALREADY_EXISTS\"}")) {
      ApiException error = badRequest(body);
      doThrow(error).when(mockTablesApi).createTable(any());

      assertThatThrownBy(
              () -> proxy.createTable(ident, columns, new Transform[0], PARQUET_AT_LOCATION))
          .isSameAs(error);
    }
  }

  @Test
  public void testCreateTableMapsConflictStatusToTableAlreadyExists() throws Exception {
    when(mockTablesApi.createTable(any())).thenThrow(new ApiException(409, "conflict"));

    assertThatThrownBy(
            () ->
                proxy.createTable(
                    Identifier.of(NAMESPACE, "t1"),
                    new Column[] {Column.create("c", DataTypes.StringType)},
                    new Transform[0],
                    PARQUET_AT_LOCATION))
        .isInstanceOf(TableAlreadyExistsException.class);
  }

  /**
   * A namespace without a schema must fail before any RPC, with an error that names the problem.
   */
  @Test
  public void testEmptyNamespaceIsRejectedBeforeAnyRpc() {
    String[] empty = new String[0];
    Identifier unqualified = Identifier.of(empty, "t1");
    Column[] columns = {Column.create("c", DataTypes.StringType)};
    List<ThrowingCallable> namespaceCalls =
        List.of(
            () -> proxy.listTables(empty),
            () -> proxyNs.loadNamespaceMetadata(empty),
            () -> proxyNs.createNamespace(empty, Map.of()),
            () -> proxyNs.dropNamespace(empty, false));

    for (ThrowingCallable call : namespaceCalls) {
      assertThatThrownBy(call)
          .isInstanceOf(ApiException.class)
          .hasMessage("Empty namespaces are not supported: name a schema, as in catalog.schema");
    }
    assertThatThrownBy(
            () -> proxy.createTable(unqualified, columns, new Transform[0], PARQUET_AT_LOCATION))
        .isInstanceOf(ApiException.class)
        .hasMessage(
            "Empty namespaces are not supported: name a schema for t1, as in catalog.schema.t1");
    verifyNoInteractions(mockTablesApi, mockSchemasApi);
  }

  private static ApiException badRequest(String body) {
    return new ApiException(400, "Bad Request", null, body);
  }

  // -- relation cache tests --

  /**
   * Spark caches the relation it resolves for a non-Delta table under the table's name, so DROP and
   * CREATE in this session, and REFRESH TABLE after another client moved the table, must clear that
   * entry, or the session keeps reading the files at the table's previous location. UC is faked by
   * a map that the test edits directly to stand for another client. The mixed-case name checks the
   * entry is found under the name as UC spells it, not a lowercased one.
   */
  @ParameterizedTest
  @CsvSource({
    "spark_catalog, t",
    "spark_catalog, MixedCase_T",
    CATALOG_NAME + ", t",
    CATALOG_NAME + ", MixedCase_T"
  })
  public void testReadFollowsTableRecreatedAtAnotherLocation(
      String catalogName, String table, @TempDir Path dir) throws Exception {
    UCProxyTestFixture fixture = new UCProxyTestFixture().build(catalogName);
    Map<String, TableInfo> tables = new HashMap<>();
    fakeTableStore(fixture.mockTablesApi, tables);
    String tableName = String.join(".", catalogName, SCHEMA_NAME, table);
    SparkSession spark = sessionWithCatalog(catalogName);
    try {
      routeToProxy(spark, catalogName, (UCProxy) fixture.proxy);
      String createSql = "CREATE TABLE " + tableName + " (id INT) USING PARQUET LOCATION '%s'";

      spark.sql(String.format(createSql, writeId(spark, dir, 0)));
      assertThat(readId(spark, tableName)).isEqualTo(0);

      spark.sql("DROP TABLE " + tableName);
      tables.put(tableName, parquetTable(catalogName, SCHEMA_NAME, table, writeId(spark, dir, 1)));
      assertThat(readId(spark, tableName)).isEqualTo(1);

      tables.remove(tableName);
      spark.sql(String.format(createSql, writeId(spark, dir, 2)));
      assertThat(readId(spark, tableName)).isEqualTo(2);

      tables.put(tableName, parquetTable(catalogName, SCHEMA_NAME, table, writeId(spark, dir, 3)));
      spark.sql("REFRESH TABLE " + tableName);
      assertThat(readId(spark, tableName)).isEqualTo(3);
    } finally {
      spark.stop();
    }
  }

  // -- unqualified names --

  /**
   * With a UC catalog as the current catalog, chosen at startup or by SET CATALOG, a name without a
   * schema resolves against schema default, as in Spark's session catalog. USE still picks another
   * schema. A name with a catalog but no schema still resolves to no table.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testUnqualifiedNamesUseDefaultSchema(boolean defaultCatalogConf, @TempDir Path dir)
      throws Exception {
    Map<String, TableInfo> tables = new HashMap<>();
    fakeTableStore(mockTablesApi, tables);
    when(mockTablesApi.listTables(eq(CATALOG_NAME), anyString(), anyInt(), any(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              String prefix = CATALOG_NAME + "." + invocation.getArgument(1) + ".";
              return new ListTablesResponse()
                  .tables(
                      tables.entrySet().stream()
                          .filter(entry -> entry.getKey().startsWith(prefix))
                          .map(Map.Entry::getValue)
                          .collect(Collectors.toList()));
            });
    when(mockSchemasApi.getSchema(CATALOG_NAME + ".other", false))
        .thenReturn(new SchemaInfo().catalogName(CATALOG_NAME).name("other").properties(Map.of()));
    SparkSession spark = sessionWithCatalog(CATALOG_NAME);
    try {
      routeToProxy(spark, CATALOG_NAME, (UCProxy) proxy);
      String createSql = "CREATE TABLE t (id INT) USING PARQUET LOCATION '%s'";
      String location0 = writeId(spark, dir, 0);
      String location1 = writeId(spark, dir, 1);
      if (defaultCatalogConf) {
        spark.conf().set("spark.sql.defaultCatalog", CATALOG_NAME);
      } else {
        spark.sql("SET CATALOG " + CATALOG_NAME);
      }

      assertThat(spark.sql("SELECT current_schema()").head().getString(0)).isEqualTo("default");
      spark.sql(String.format(createSql, location0));
      assertThat(tables).containsOnlyKeys(CATALOG_NAME + ".default.t");
      assertThat(readId(spark, "t")).isEqualTo(0);
      assertThat(spark.sql("SHOW TABLES").select("tableName").as(Encoders.STRING()).collectAsList())
          .containsExactly("t");

      String noSchema = CATALOG_NAME + ".t";
      spark.sql("DROP TABLE IF EXISTS " + noSchema);
      assertTableOrViewNotFound(() -> spark.sql("DROP TABLE " + noSchema));
      assertTableOrViewNotFound(() -> spark.sql("SELECT * FROM " + noSchema));
      assertThatThrownBy(
              () ->
                  spark.sql(
                      "CREATE TABLE IF NOT EXISTS "
                          + noSchema
                          + String.format(" (id INT) USING PARQUET LOCATION '%s'", location1)))
          .isInstanceOf(ApiException.class)
          .hasMessage(
              "Empty namespaces are not supported: name a schema for t, as in catalog.schema.t");
      assertThat(tables).containsOnlyKeys(CATALOG_NAME + ".default.t");

      spark.sql("USE other");
      spark.sql(String.format(createSql, location1));
      assertThat(tables).containsOnlyKeys(CATALOG_NAME + ".default.t", CATALOG_NAME + ".other.t");
      assertThat(readId(spark, "t")).isEqualTo(1);

      spark.sql("DROP TABLE t");
      spark.sql("DROP TABLE IF EXISTS t");
      assertThat(tables).containsOnlyKeys(CATALOG_NAME + ".default.t");
    } finally {
      spark.stop();
    }
  }

  private static void assertTableOrViewNotFound(ThrowingCallable sql) {
    assertThatThrownBy(sql)
        .isInstanceOfSatisfying(
            AnalysisException.class,
            e -> assertThat(e.getCondition()).isEqualTo("TABLE_OR_VIEW_NOT_FOUND"));
  }

  private static void fakeTableStore(TablesApi tablesApi, Map<String, TableInfo> tables)
      throws ApiException {
    when(tablesApi.getTable(anyString(), anyBoolean(), anyBoolean(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              TableInfo table = tables.get(invocation.<String>getArgument(0));
              if (table == null) {
                throw new ApiException(404, "not found");
              }
              return table;
            });
    when(tablesApi.createTable(any()))
        .thenAnswer(
            invocation -> {
              CreateTable create = invocation.getArgument(0);
              TableInfo table =
                  parquetTable(
                      create.getCatalogName(),
                      create.getSchemaName(),
                      create.getName(),
                      create.getStorageLocation());
              tables.put(
                  String.join(
                      ".", create.getCatalogName(), create.getSchemaName(), create.getName()),
                  table);
              return table;
            });
    when(tablesApi.deleteTable(anyString()))
        .thenAnswer(invocation -> tables.remove(invocation.<String>getArgument(0)));
  }

  private static TableInfo parquetTable(
      String catalogName, String schemaName, String table, String location) {
    return new TableInfo()
        .catalogName(catalogName)
        .schemaName(schemaName)
        .name(table)
        .tableId("table-id-" + table)
        .tableType(TableType.EXTERNAL)
        .dataSourceFormat(DataSourceFormat.PARQUET)
        .storageLocation(location)
        .columns(List.of(intColumn("id", 0, null)));
  }

  /** Writes a Parquet file holding only {@code id} to a new directory and returns its URI. */
  private static String writeId(SparkSession spark, Path dir, int id) {
    String location = dir.resolve("id" + id).toUri().toString();
    spark.sql("SELECT " + id + " AS id").write().parquet(location);
    return location;
  }

  private static int readId(SparkSession spark, String tableName) {
    return spark.sql("SELECT id FROM " + tableName).head().getInt(0);
  }
}
