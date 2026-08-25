package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import scala.Option;

/**
 * Tests for the Delta-transaction-log schema recovery fix in {@link UCProxy}.
 *
 * <p>Background: when {@code DeltaCatalog} is on the classpath, it commits the real schema to
 * the table's {@code _delta_log} itself and then calls back into {@link UCProxy}'s legacy {@code
 * createTable(Identifier, StructType, ...)} overload with an <em>empty</em> schema, since it
 * treats the transaction log (not the catalog) as the schema source of truth. Left unhandled,
 * that leaves Unity Catalog's own table record with zero columns. {@link
 * UCProxy#readSchemaFromDeltaLog} recovers the real schema by reading it back from the commit
 * Delta already wrote, so UC's metadata matches reality.
 */
public class UCProxyDeltaSchemaRecoveryTest {

  private static final Identifier IDENT = Identifier.of(new String[] {"schema"}, "table");
  private static final Transform[] PARTITIONS = new Transform[0];
  private static final StructType EMPTY_SCHEMA = new StructType();
  private static final StructType ID_SCHEMA =
      new StructType().add("id", DataTypes.IntegerType, false);
  private static final StructType ID_NAME_SCHEMA =
      new StructType()
          .add("id", DataTypes.IntegerType, false)
          .add("name", DataTypes.StringType, true);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private TablesApi tablesApi;
  private UCProxy ucProxy;

  @BeforeEach
  public void setUp() throws Exception {
    tablesApi = mock(TablesApi.class);
    ApiClient apiClient = mock(ApiClient.class);
    TokenProvider tokenProvider =
        TokenProvider.create(Map.of("type", "static", "token", "tok"));

    ucProxy =
        new UCProxy(
            URI.create("http://localhost"),
            tokenProvider,
            /* renewCredEnabled= */ false,
            /* credScopedFsEnabled= */ true,
            /* serverSidePlanningEnabled= */ false,
            apiClient,
            tablesApi);
    ucProxy.initialize("main", new CaseInsensitiveStringMap(Map.of()));
  }

  // ---------------------------------------------------------------------
  // readSchemaFromDeltaLog
  // ---------------------------------------------------------------------

  @Test
  public void testReadSchemaFromDeltaLogRecoversSchemaFromSingleCommit(@TempDir File tempDir)
      throws Exception {
    String location = fileLocation(tempDir);
    writeCommit(tempDir, "00000000000000000000.json", protocolLine(), metaDataLine(ID_SCHEMA));

    Option<StructType> result =
        ucProxy.readSchemaFromDeltaLog(Map.of(TableCatalog.PROP_LOCATION, location));

    assertThat(result.isDefined()).isTrue();
    assertSchemaEquals(ID_SCHEMA, result.get());
  }

  @Test
  public void testReadSchemaFromDeltaLogUsesLastCommitWhenSchemaEvolves(@TempDir File tempDir)
      throws Exception {
    String location = fileLocation(tempDir);
    writeCommit(tempDir, "00000000000000000000.json", protocolLine(), metaDataLine(ID_SCHEMA));
    writeCommit(tempDir, "00000000000000000001.json", metaDataLine(ID_NAME_SCHEMA));

    Option<StructType> result =
        ucProxy.readSchemaFromDeltaLog(Map.of(TableCatalog.PROP_LOCATION, location));

    assertThat(result.isDefined()).isTrue();
    assertSchemaEquals(ID_NAME_SCHEMA, result.get());
  }

  @Test
  public void testReadSchemaFromDeltaLogToleratesNonMetadataActions(@TempDir File tempDir)
      throws Exception {
    // Realistic commit file: protocol + metaData + commitInfo actions on separate lines.
    String location = fileLocation(tempDir);
    writeCommit(
        tempDir,
        "00000000000000000000.json",
        protocolLine(),
        metaDataLine(ID_SCHEMA),
        commitInfoLine());

    Option<StructType> result =
        ucProxy.readSchemaFromDeltaLog(Map.of(TableCatalog.PROP_LOCATION, location));

    assertThat(result.isDefined()).isTrue();
    assertSchemaEquals(ID_SCHEMA, result.get());
  }

  @Test
  public void testReadSchemaFromDeltaLogReturnsEmptyWhenDeltaLogDirMissing(@TempDir File tempDir) {
    String location = fileLocation(tempDir); // tempDir exists but has no _delta_log subdir

    Option<StructType> result =
        ucProxy.readSchemaFromDeltaLog(Map.of(TableCatalog.PROP_LOCATION, location));

    assertThat(result.isEmpty()).isTrue();
  }

  @Test
  public void testReadSchemaFromDeltaLogReturnsEmptyWhenLocationPropertyMissing() {
    Option<StructType> result = ucProxy.readSchemaFromDeltaLog(Map.of());

    assertThat(result.isEmpty()).isTrue();
  }

  @Test
  public void testReadSchemaFromDeltaLogReturnsEmptyWhenCommitLineIsMalformed(
      @TempDir File tempDir) throws Exception {
    String location = fileLocation(tempDir);
    // A malformed (non-JSON) line anywhere in the commit aborts recovery entirely -- this method
    // fails soft for the whole operation rather than skipping just the bad line, since a
    // truncated/corrupt commit file shouldn't be partially trusted.
    writeCommit(tempDir, "00000000000000000000.json", "not-json", metaDataLine(ID_SCHEMA));

    Option<StructType> result =
        ucProxy.readSchemaFromDeltaLog(Map.of(TableCatalog.PROP_LOCATION, location));

    assertThat(result.isEmpty()).isTrue();
  }

  // ---------------------------------------------------------------------
  // createTable(ident, schema: StructType, ...) -- the legacy overload DeltaCatalog calls back
  // into.
  // ---------------------------------------------------------------------

  @Test
  public void testCreateTableRecoversSchemaFromDeltaLogWhenIncomingSchemaEmpty(
      @TempDir File tempDir) throws Exception {
    String location = fileLocation(tempDir);
    writeCommit(tempDir, "00000000000000000000.json", metaDataLine(ID_NAME_SCHEMA));
    mockGetTableForLoadTable(location, ID_NAME_SCHEMA, TableType.EXTERNAL);

    ucProxy.createTable(
        IDENT,
        EMPTY_SCHEMA,
        PARTITIONS,
        Map.of(
            TableCatalog.PROP_PROVIDER, "delta",
            TableCatalog.PROP_LOCATION, location));

    ArgumentCaptor<CreateTable> captor = ArgumentCaptor.forClass(CreateTable.class);
    verify(tablesApi).createTable(captor.capture());
    List<ColumnInfo> sentColumns = captor.getValue().getColumns();

    assertThat(sentColumns).hasSize(2);
    assertThat(sentColumns.get(0).getName()).isEqualTo("id");
    assertThat(sentColumns.get(0).getTypeName()).isEqualTo(ColumnTypeName.INT);
    assertThat(sentColumns.get(0).getNullable()).isFalse();
    assertThat(sentColumns.get(1).getName()).isEqualTo("name");
    assertThat(sentColumns.get(1).getTypeName()).isEqualTo(ColumnTypeName.STRING);
    assertThat(sentColumns.get(1).getNullable()).isTrue();
  }

  @Test
  public void testCreateTableDoesNotOverrideNonEmptySchema(@TempDir File tempDir)
      throws Exception {
    String location = fileLocation(tempDir);
    // Delta log at this location disagrees with the schema passed in -- proves the recovery path
    // is only consulted when the incoming schema is empty, never used to override a populated one.
    writeCommit(tempDir, "00000000000000000000.json", metaDataLine(ID_NAME_SCHEMA));
    mockGetTableForLoadTable(location, ID_SCHEMA, TableType.EXTERNAL);

    ucProxy.createTable(
        IDENT,
        ID_SCHEMA,
        PARTITIONS,
        Map.of(
            TableCatalog.PROP_PROVIDER, "delta",
            TableCatalog.PROP_LOCATION, location));

    ArgumentCaptor<CreateTable> captor = ArgumentCaptor.forClass(CreateTable.class);
    verify(tablesApi).createTable(captor.capture());
    List<ColumnInfo> sentColumns = captor.getValue().getColumns();

    assertThat(sentColumns).hasSize(1);
    assertThat(sentColumns.get(0).getName()).isEqualTo("id");
  }

  @Test
  public void testCreateTableNonDeltaProviderWithEmptySchemaDoesNotAttemptRecovery(
      @TempDir File tempDir) throws Exception {
    String location = fileLocation(tempDir);
    // A valid _delta_log happens to exist here, but the provider isn't delta, so recovery must
    // not kick in -- confirms the gate is provider-aware, not just "schema is empty".
    writeCommit(tempDir, "00000000000000000000.json", metaDataLine(ID_NAME_SCHEMA));
    mockGetTableForLoadTable(location, EMPTY_SCHEMA, TableType.EXTERNAL);

    ucProxy.createTable(
        IDENT,
        EMPTY_SCHEMA,
        PARTITIONS,
        Map.of(
            TableCatalog.PROP_PROVIDER, "parquet",
            TableCatalog.PROP_LOCATION, location));

    ArgumentCaptor<CreateTable> captor = ArgumentCaptor.forClass(CreateTable.class);
    verify(tablesApi).createTable(captor.capture());

    assertThat(captor.getValue().getColumns()).isEmpty();
  }

  // ---------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------

  private static String fileLocation(File dir) {
    return "file://" + dir.getAbsolutePath();
  }

  private static void writeCommit(File tempDir, String fileName, String... lines)
      throws Exception {
    File deltaLogDir = new File(tempDir, "_delta_log");
    deltaLogDir.mkdirs();
    File commitFile = new File(deltaLogDir, fileName);
    Files.write(
        commitFile.toPath(), String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
  }

  private static String metaDataLine(StructType schema) throws Exception {
    Map<String, Object> metaData = new LinkedHashMap<>();
    metaData.put("id", "meta-id");
    metaData.put("format", Map.of("provider", "parquet", "options", Map.of()));
    metaData.put("schemaString", schema.json());
    metaData.put("partitionColumns", Collections.emptyList());
    metaData.put("configuration", Collections.emptyMap());
    metaData.put("createdTime", 1234567890L);

    Map<String, Object> line = new LinkedHashMap<>();
    line.put("metaData", metaData);
    return MAPPER.writeValueAsString(line);
  }

  private static String protocolLine() throws Exception {
    Map<String, Object> protocol = Map.of("minReaderVersion", 1, "minWriterVersion", 2);
    Map<String, Object> line = new LinkedHashMap<>();
    line.put("protocol", protocol);
    return MAPPER.writeValueAsString(line);
  }

  private static String commitInfoLine() throws Exception {
    Map<String, Object> commitInfo = Map.of("timestamp", 1234567890L, "operation", "CREATE TABLE");
    Map<String, Object> line = new LinkedHashMap<>();
    line.put("commitInfo", commitInfo);
    return MAPPER.writeValueAsString(line);
  }

  /**
   * Mocks {@code tablesApi.getTable(...)} so the {@code loadTable(ident)} call at the end of
   * {@code UCProxy.createTable} succeeds. The returned {@link TableInfo}'s contents aren't the
   * subject of these tests (the request captured via {@link CreateTable} is); this only needs to
   * be internally consistent enough that {@code loadV1Table} doesn't throw.
   */
  private void mockGetTableForLoadTable(String location, StructType schema, TableType tableType) {
    List<ColumnInfo> columns =
        java.util.Arrays.stream(schema.fields())
            .map(UCProxyDeltaSchemaRecoveryTest::toColumnInfo)
            .collect(java.util.stream.Collectors.toList());

    TableInfo tableInfo =
        new TableInfo()
            .name("table")
            .schemaName("schema")
            .catalogName("main")
            .tableType(tableType)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(location)
            .tableId("table-id")
            .properties(Collections.emptyMap())
            .columns(columns)
            .createdAt(0L);

    when(tablesApi.getTable(eq("main.schema.table"), any(), any())).thenReturn(tableInfo);
  }

  private static ColumnInfo toColumnInfo(StructField field) {
    ColumnInfo column = new ColumnInfo();
    column.setName(field.name());
    column.setNullable(field.nullable());
    column.setTypeText(field.dataType().catalogString());
    column.setTypeName(
        field.dataType() == DataTypes.IntegerType ? ColumnTypeName.INT : ColumnTypeName.STRING);
    return column;
  }

  private static void assertSchemaEquals(StructType expected, StructType actual) {
    assertThat(actual.fields()).hasSize(expected.fields().length);
    for (int i = 0; i < expected.fields().length; i++) {
      StructField expectedField = expected.fields()[i];
      StructField actualField = actual.fields()[i];
      assertThat(actualField.name()).isEqualTo(expectedField.name());
      assertThat(actualField.dataType()).isEqualTo(expectedField.dataType());
      assertThat(actualField.nullable()).isEqualTo(expectedField.nullable());
    }
  }
}
