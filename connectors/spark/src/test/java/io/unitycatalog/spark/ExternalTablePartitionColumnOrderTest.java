package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Partitioned external tables whose partition columns are not declared last. Spark reads and writes
 * a non-Delta table as its data columns followed by its partition columns, so DESCRIBE TABLE must
 * list the columns in that order too, while UC keeps the declared order.
 */
public class ExternalTablePartitionColumnOrderTest extends BaseSparkIntegrationTest {
  private static final String TABLE = CATALOG_NAME + "." + SCHEMA_NAME + ".part_order";
  private static final List<String> FORMATS = List.of("PARQUET", "ORC", "JSON", "CSV");

  @TempDir private File dataDir;

  @BeforeEach
  public void createSession() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
  }

  /** Every layout declares data columns a, b, q and partition column p, or partitions by q, p. */
  private enum Layout {
    PARTITION_IN_MIDDLE(
        "a INT, p INT COMMENT 'part key', b STRING DEFAULT 'dflt', q STRING",
        "p",
        "PARTITION (p = 30) VALUES (3, 'three', 'z')",
        "PARTITION (p) VALUES (4, 'four', 'w', 40)"),
    PARTITION_FIRST(
        "p INT COMMENT 'part key', a INT, b STRING DEFAULT 'dflt', q STRING",
        "p",
        "PARTITION (p = 30) VALUES (3, 'three', 'z')",
        "PARTITION (p) VALUES (4, 'four', 'w', 40)"),
    TWO_PARTITIONS_NOT_IN_DECLARED_ORDER(
        "a INT, p INT COMMENT 'part key', b STRING DEFAULT 'dflt', q STRING",
        "q, p",
        "PARTITION (p = 30, q = 'z') VALUES (3, 'three')",
        "PARTITION (q = 'w', p) VALUES (4, 'four', 40)");

    final String columns;
    final String partitionedBy;
    final String staticPartitionInsert;
    final String dynamicPartitionInsert;

    Layout(
        String columns,
        String partitionedBy,
        String staticPartitionInsert,
        String dynamicPartitionInsert) {
      this.columns = columns;
      this.partitionedBy = partitionedBy;
      this.staticPartitionInsert = staticPartitionInsert;
      this.dynamicPartitionInsert = dynamicPartitionInsert;
    }

    String partitionDir(String q, int p) {
      return this == TWO_PARTITIONS_NOT_IN_DECLARED_ORDER ? "q=" + q + "/p=" + p : "p=" + p;
    }
  }

  private static Stream<Arguments> formatsAndLayouts() {
    return FORMATS.stream()
        .flatMap(format -> Arrays.stream(Layout.values()).map(l -> Arguments.of(format, l)));
  }

  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("formatsAndLayouts")
  public void testDescribeListsColumnsInTheOrderOfReadsAndWrites(String format, Layout layout)
      throws IOException {
    sql(
        "CREATE TABLE %s (%s) USING %s PARTITIONED BY (%s) LOCATION '%s'",
        TABLE, layout.columns, format, layout.partitionedBy, dataDir.getAbsolutePath());

    assertThat(describeColumns())
        .containsExactly(
            RowFactory.create("a", "int", null),
            RowFactory.create("b", "string", null),
            RowFactory.create("q", "string", null),
            RowFactory.create("p", "int", "part key"));
    assertThat(session.table(TABLE).columns()).containsExactly("a", "b", "q", "p");

    sql("INSERT INTO %s VALUES (1, 'one', 'x', 10)", TABLE);
    sql("INSERT INTO %s (p, q, a) VALUES (20, 'y', 2)", TABLE);
    sql("INSERT INTO %s %s", TABLE, layout.staticPartitionInsert);
    sql("INSERT INTO %s %s", TABLE, layout.dynamicPartitionInsert);

    List<Row> expected =
        List.of(
            RowFactory.create(1, "one", "x", 10),
            RowFactory.create(2, "dflt", "y", 20),
            RowFactory.create(3, "three", "z", 30),
            RowFactory.create(4, "four", "w", 40));
    assertThat(sql("SELECT a, b, q, p FROM %s ORDER BY a", TABLE))
        .containsExactlyElementsOf(expected);
    assertThat(readFiles(format)).containsExactlyElementsOf(expected);
    assertThat(partitionDirs())
        .containsExactlyInAnyOrder(
            layout.partitionDir("x", 10),
            layout.partitionDir("y", 20),
            layout.partitionDir("z", 30),
            layout.partitionDir("w", 40));

    sql("INSERT OVERWRITE %s VALUES (5, 'five', 'v', 50)", TABLE);

    List<Row> overwritten = List.of(RowFactory.create(5, "five", "v", 50));
    assertThat(sql("SELECT a, b, q, p FROM %s", TABLE)).containsExactlyElementsOf(overwritten);
    assertThat(readFiles(format)).containsExactlyElementsOf(overwritten);
    assertThat(partitionDirs()).containsExactly(layout.partitionDir("v", 50));
  }

  /** The column rows of DESCRIBE TABLE, without the partition section. */
  private List<Row> describeColumns() {
    return sql("DESCRIBE TABLE %s", TABLE).stream()
        .takeWhile(row -> !row.getString(0).startsWith("#"))
        .collect(Collectors.toList());
  }

  /** Reads the table's files directly, without the catalog. */
  private List<Row> readFiles(String format) {
    return session
        .read()
        .format(format)
        .schema("a INT, b STRING, q STRING, p INT")
        .load(dataDir.getAbsolutePath())
        .select("a", "b", "q", "p")
        .orderBy("a")
        .collectAsList();
  }

  /** The directories, relative to the table location, that hold data files. */
  private List<String> partitionDirs() throws IOException {
    Path root = dataDir.toPath();
    try (Stream<Path> files = Files.walk(root)) {
      return files
          .filter(f -> f.getFileName().toString().startsWith("part-"))
          .map(f -> root.relativize(f.getParent()).toString().replace(File.separatorChar, '/'))
          .distinct()
          .collect(Collectors.toList());
    }
  }

  private static List<String> formats() {
    return FORMATS;
  }

  @ParameterizedTest
  @MethodSource("formats")
  public void testReadsFilesOfTableCreatedThroughRestApi(String format) throws ApiException {
    session
        .sql("SELECT 1 AS a, 'one' AS b, 10 AS p UNION ALL SELECT 2, 'two', 20")
        .write()
        .partitionBy("p")
        .format(format)
        .mode("overwrite")
        .save(dataDir.getAbsolutePath());
    new SdkTableOperations(createApiClient(serverConfig))
        .createTable(
            new CreateTable()
                .name("part_order")
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .tableType(TableType.EXTERNAL)
                .dataSourceFormat(DataSourceFormat.fromValue(format))
                .storageLocation(dataDir.getAbsolutePath())
                .columns(
                    List.of(
                        column("a", "int", "integer", ColumnTypeName.INT, 0),
                        column("p", "int", "integer", ColumnTypeName.INT, 1).partitionIndex(0),
                        column("b", "string", "string", ColumnTypeName.STRING, 2))));

    assertThat(describeColumns())
        .containsExactly(
            RowFactory.create("a", "int", null),
            RowFactory.create("b", "string", null),
            RowFactory.create("p", "int", null));
    assertThat(sql("SELECT * FROM %s ORDER BY a", TABLE))
        .containsExactly(RowFactory.create(1, "one", 10), RowFactory.create(2, "two", 20));
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
  public void testDeltaTableKeepsDeclaredColumnOrder() throws IOException {
    sql(
        "CREATE TABLE %s (a INT, p INT, b STRING) USING DELTA PARTITIONED BY (p) LOCATION '%s'",
        TABLE, dataDir.getAbsolutePath());
    sql("INSERT INTO %s VALUES (1, 2, 'x')", TABLE);

    assertThat(describeColumns())
        .containsExactly(
            RowFactory.create("a", "int", null),
            RowFactory.create("p", "int", null),
            RowFactory.create("b", "string", null));
    assertThat(sql("SELECT a, p, b FROM %s", TABLE)).containsExactly(RowFactory.create(1, 2, "x"));
    assertThat(partitionDirs()).containsExactly("p=2");
  }
}
