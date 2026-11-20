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
import static org.mockito.Mockito.verify;
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
import io.unitycatalog.spark.utils.OptionsUtil;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
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
    SparkSession spark = startSession(catalogName, (UCProxy) fixture.proxy);
    try {
      String createSql = "CREATE TABLE " + tableName + " (id INT) USING PARQUET LOCATION '%s'";

      spark.sql(String.format(createSql, writeId(spark, dir, 0)));
      assertThat(readId(spark, tableName)).isEqualTo(0);

      spark.sql("DROP TABLE " + tableName);
      tables.put(tableName, parquetTable(catalogName, table, writeId(spark, dir, 1)));
      assertThat(readId(spark, tableName)).isEqualTo(1);

      tables.remove(tableName);
      spark.sql(String.format(createSql, writeId(spark, dir, 2)));
      assertThat(readId(spark, tableName)).isEqualTo(2);

      tables.put(tableName, parquetTable(catalogName, table, writeId(spark, dir, 3)));
      spark.sql("REFRESH TABLE " + tableName);
      assertThat(readId(spark, tableName)).isEqualTo(3);
    } finally {
      spark.stop();
    }
  }

  /**
   * Spark hands each {@code OPTIONS} entry to the catalog as {@code option.<key>}, so the CSV
   * {@code comment} option must reach the reader even though {@code comment} is also the reserved
   * table comment key. Both tables read the same file. Only the second one has a table comment, set
   * by {@code COMMENT}.
   */
  @Test
  public void testCsvCommentOptionSkipsCommentLines(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("data.csv"), "# exported by tool\nx,1\ny,2\n");
    fakeTableStore(mockTablesApi, new HashMap<>());
    String optionOnly = String.join(".", CATALOG_NAME, SCHEMA_NAME, "option_only");
    String withComment = String.join(".", CATALOG_NAME, SCHEMA_NAME, "with_comment");
    SparkSession spark = startSession(CATALOG_NAME, (UCProxy) proxy);
    try {
      spark.sql(
          String.format(
              "CREATE TABLE %s (s STRING, i INT) USING CSV OPTIONS (comment '#') LOCATION '%s'",
              optionOnly, dir.toUri()));
      spark.sql(
          String.format(
              "CREATE TABLE %s (s STRING, i INT) USING CSV COMMENT 'Exported rows'"
                  + " OPTIONS (comment '#') LOCATION '%s'",
              withComment, dir.toUri()));

      for (String tableName : List.of(optionOnly, withComment)) {
        assertThat(spark.sql("SELECT s, i FROM " + tableName + " ORDER BY s").collectAsList())
            .containsExactly(RowFactory.create("x", 1), RowFactory.create("y", 2));
      }
      assertThat(spark.catalog().getTable(optionOnly).description()).isNull();
      assertThat(spark.catalog().getTable(withComment).description()).isEqualTo("Exported rows");
    } finally {
      spark.stop();
    }
  }

  /**
   * Spark passes {@code OPTIONS (comment '#')} to the catalog both as {@code option.comment} and as
   * the bare table comment key. A non-Delta table does not take that copy as its comment, while a
   * Delta table keeps the comment it was given.
   */
  @ParameterizedTest
  @CsvSource({"csv,", "delta,#"})
  public void testCreateTableCommentEqualToCommentOption(String provider, String expectedComment)
      throws Exception {
    Map<String, TableInfo> tables = new HashMap<>();
    fakeTableStore(mockTablesApi, tables);
    String name = "t_" + provider;

    proxy.createTable(
        Identifier.of(NAMESPACE, name),
        new StructType().add("i", DataTypes.IntegerType),
        new Transform[0],
        Map.of(
            "provider",
            provider,
            "location",
            "file:/tmp/uc/" + name,
            "comment",
            "#",
            "option.comment",
            "#"));

    assertThat(tables.get("test_catalog.test_schema." + name).getComment())
        .isEqualTo(expectedComment);
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
                  new TableInfo()
                      .catalogName(create.getCatalogName())
                      .schemaName(create.getSchemaName())
                      .name(create.getName())
                      .tableId("table-id-" + create.getName())
                      .tableType(create.getTableType())
                      .dataSourceFormat(create.getDataSourceFormat())
                      .storageLocation(create.getStorageLocation())
                      .comment(create.getComment())
                      .properties(create.getProperties())
                      .columns(create.getColumns());
              tables.put(
                  String.join(
                      ".", create.getCatalogName(), create.getSchemaName(), create.getName()),
                  table);
              return table;
            });
    when(tablesApi.deleteTable(anyString()))
        .thenAnswer(invocation -> tables.remove(invocation.<String>getArgument(0)));
  }

  /**
   * Starts a session whose {@link UCSingleCatalog} named {@code catalogName} reaches UC only
   * through {@code proxy}, by way of its Delta catalog.
   */
  private static SparkSession startSession(String catalogName, UCProxy proxy) {
    String catalogConf = "spark.sql.catalog." + catalogName;
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
    SparkSession spark =
        SparkSession.builder()
            .master("local[1]")
            .config(catalogConf, UCSingleCatalog.class.getName())
            .config(catalogConf + "." + OptionsUtil.URI, "http://localhost:8080")
            .config(catalogConf + "." + OptionsUtil.TOKEN, "token")
            // Delta's own UC client would load tables from the server instead of the fake proxy.
            .config(catalogConf + "." + OptionsUtil.DELTA_API_ENABLED, "false")
            .getOrCreate();
    UCSingleCatalog catalog =
        (UCSingleCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    catalog.ucProxy_$eq(proxy);
    ((DelegatingCatalogExtension) catalog.delegate()).setDelegateCatalog(proxy);
    return spark;
  }

  private static TableInfo parquetTable(String catalogName, String table, String location) {
    return new TableInfo()
        .catalogName(catalogName)
        .schemaName(SCHEMA_NAME)
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
