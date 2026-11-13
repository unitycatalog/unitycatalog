package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/** COMMENT, TBLPROPERTIES and OPTIONS of external tables as Spark sees them after a reload. */
public class ExternalTableMetadataTest extends BaseSparkIntegrationTest {
  private static final String TABLE = CATALOG_NAME + "." + SCHEMA_NAME + ".meta_table";

  @TempDir private File dataDir;

  @BeforeEach
  public void createSession() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
  }

  private static List<Arguments> formatsAndPartitioning() {
    return List.of("PARQUET", "ORC", "JSON", "CSV").stream()
        .flatMap(
            format -> List.of(Arguments.of(format, false), Arguments.of(format, true)).stream())
        .collect(Collectors.toList());
  }

  /**
   * {@code pathGlobFilter} is a reader option of every file source, so it shows whether a table
   * property is (wrongly) applied when the table is read: if it were, no file would match.
   */
  @ParameterizedTest(name = "{0}, partitioned={1}")
  @MethodSource("formatsAndPartitioning")
  public void testCommentAndTablePropertiesSurviveReload(String format, boolean partitioned)
      throws AnalysisException {
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING %s %s COMMENT 'Sales \"Q3\" café 日本"
            + " it\\'s' TBLPROPERTIES ('pathGlobFilter' = '*.nothing', 'Owner.Team' = 'Data Eng')"
            + " LOCATION '%s'",
        TABLE, format, partitioned ? "PARTITIONED BY (s)" : "", dataDir.getAbsolutePath());
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", TABLE);

    assertThat(sql("SELECT i, s FROM %s ORDER BY i", TABLE))
        .containsExactly(RowFactory.create(1, "a"), RowFactory.create(2, "b"));
    assertThat(session.catalog().getTable(TABLE).description())
        .isEqualTo("Sales \"Q3\" café 日本 it's");
    assertThat(describeExtended().get("Comment")).isEqualTo("Sales \"Q3\" café 日本 it's");
    assertThat(showTblProperties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("pathGlobFilter", "*.nothing", "Owner.Team", "Data Eng"));
    assertThat(sql("SHOW TBLPROPERTIES %s ('Owner.Team')", TABLE))
        .containsExactly(RowFactory.create("Owner.Team", "Data Eng"));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "  "})
  public void testBlankOrMissingComment(String comment) throws AnalysisException {
    sql(
        "CREATE TABLE %s (i INT) USING PARQUET %s LOCATION '%s'",
        TABLE, comment == null ? "" : "COMMENT '" + comment + "'", dataDir.getAbsolutePath());

    assertThat(session.catalog().getTable(TABLE).description()).isEqualTo(comment);
    assertThat(describeExtended().get("Comment")).isEqualTo(comment);
  }

  @Test
  public void testCsvOptionsStayReaderOptions() throws IOException {
    Files.writeString(
        new File(dataDir, "part-0.csv").toPath(), "id;name\n1;a\n2;b\n", StandardCharsets.UTF_8);
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING CSV OPTIONS (Header 'true', delimiter ';')"
            + " TBLPROPERTIES ('k' = 'v') LOCATION '%s'",
        TABLE, dataDir.getAbsolutePath());

    assertThat(sql("SELECT i, s FROM %s ORDER BY i", TABLE))
        .containsExactly(RowFactory.create(1, "a"), RowFactory.create(2, "b"));
    assertThat(showTblProperties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("k", "v", "option.Header", "true", "option.delimiter", ";"));
  }

  @Test
  public void testCsvHeaderTablePropertyIsNotAReaderOption() throws IOException {
    Files.writeString(
        new File(dataDir, "part-0.csv").toPath(), "1,a\n2,b\n", StandardCharsets.UTF_8);
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING CSV TBLPROPERTIES ('header' = 'true')"
            + " LOCATION '%s'",
        TABLE, dataDir.getAbsolutePath());

    assertThat(sql("SELECT i, s FROM %s ORDER BY i", TABLE))
        .containsExactly(RowFactory.create(1, "a"), RowFactory.create(2, "b"));
    assertThat(sql("SHOW TBLPROPERTIES %s ('header')", TABLE))
        .containsExactly(RowFactory.create("header", "true"));
  }

  @Test
  public void testOptionPrefixedPropertiesFromRestApiAreReaderOptions() throws Exception {
    Files.writeString(
        new File(dataDir, "part-0.csv").toPath(), "id,name\n1,a\n2,b\n", StandardCharsets.UTF_8);
    new SdkTableOperations(createApiClient(serverConfig))
        .createTable(
            new CreateTable()
                .name("meta_table")
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .tableType(TableType.EXTERNAL)
                .dataSourceFormat(DataSourceFormat.CSV)
                .storageLocation(dataDir.getAbsolutePath())
                .columns(
                    List.of(
                        column("i", "int", "integer", ColumnTypeName.INT, 0),
                        column("s", "string", "string", ColumnTypeName.STRING, 1)))
                .properties(Map.of("option.header", "true", "k", "v")));

    assertThat(sql("SELECT i, s FROM %s ORDER BY i", TABLE))
        .containsExactly(RowFactory.create(1, "a"), RowFactory.create(2, "b"));
    assertThat(showTblProperties())
        .containsExactlyInAnyOrderEntriesOf(Map.of("k", "v", "option.header", "true"));
  }

  private static ColumnInfo column(
      String name, String typeText, String jsonType, ColumnTypeName type, int position) {
    return new ColumnInfo()
        .name(name)
        .typeText(typeText)
        .typeName(type)
        .typeJson(
            String.format(
                "{\"name\":\"%s\",\"type\":\"%s\",\"nullable\":true,\"metadata\":{}}",
                name, jsonType))
        .nullable(true)
        .position(position);
  }

  @Test
  public void testDeltaTableMetadataUnchanged() throws AnalysisException {
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING DELTA COMMENT 'delta comment'"
            + " TBLPROPERTIES ('k' = 'v') LOCATION '%s'",
        TABLE, dataDir.getAbsolutePath());
    sql("INSERT INTO %s VALUES (1, 'a')", TABLE);

    assertThat(sql("SELECT i, s FROM %s", TABLE)).containsExactly(RowFactory.create(1, "a"));
    assertThat(session.catalog().getTable(TABLE).description()).isEqualTo("delta comment");
    assertThat(sql("SHOW TBLPROPERTIES %s ('k')", TABLE))
        .containsExactly(RowFactory.create("k", "v"));
  }

  private Map<String, String> showTblProperties() {
    return sql("SHOW TBLPROPERTIES %s", TABLE).stream()
        .collect(Collectors.toMap(row -> row.getString(0), row -> row.getString(1)));
  }

  private Map<String, String> describeExtended() {
    Map<String, String> rows = new HashMap<>();
    for (Row row : sql("DESCRIBE TABLE EXTENDED %s", TABLE)) {
      rows.putIfAbsent(row.getString(0), row.getString(1));
    }
    return rows;
  }
}
