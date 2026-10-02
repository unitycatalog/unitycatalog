package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Shared base for the Spark + Iceberg end-to-end suites. It drives Iceberg tables through a real
 * Spark + Iceberg runtime against Unity Catalog's Iceberg REST catalog ({@code
 * /api/2.1/unity-catalog/iceberg}), rather than through the UC Spark connector (which has no
 * Iceberg support). Spark is configured with Iceberg's own {@code SparkCatalog} (REST), wired by
 * {@code BaseSparkIntegrationTest} for every catalog its {@code isIcebergCatalog} marks; the {@code
 * /v1/config} handshake returns a {@code prefix} so the standard Iceberg client targets UC's
 * catalog-scoped paths, exercising the server's config / namespace / create / commit / load / drop
 * endpoints exactly as an external Iceberg client would.
 *
 * <p>Storage is a local {@code file://} warehouse: the server writes the first metadata file
 * through its native local FileIO and Spark's Iceberg {@code HadoopFileIO} reads and writes data
 * files and later metadata to the same directory, so the full create / write / read / commit path
 * round-trips without cloud credentials. Cloud credential-vending is covered separately by the
 * server-side Iceberg REST catalog tests.
 *
 * <p>Concrete subclasses pick managed ({@link IcebergManagedTableReadWriteTest}) or external
 * ({@link IcebergExternalTableReadWriteTest}) tables, mirroring the Delta {@code
 * DeltaManagedTableReadWriteTest} / {@code DeltaExternalTableReadWriteTest} split. The class reuses
 * {@link BaseTableReadWriteTest}'s create/read/write matrix and helpers; the {@code
 * ExternalTableReadWriteTest} base is UC-connector-specific, so it is not reused here.
 *
 * <p>Compiled only for Spark 4.0 / 4.1: Iceberg 1.11.0 publishes no Spark 4.2 runtime, so this
 * source lives under {@code src/test/scala-shims/spark-4.0-4.1} and is absent from the 4.2 build.
 */
public abstract class IcebergTableReadWriteTest extends BaseTableReadWriteTest {

  @Override
  protected boolean isIcebergCatalog(String catalog) {
    return true;
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // Advertise and accept the Iceberg write endpoints (createTable / updateTable / dropTable);
    // otherwise the REST client refuses them as unsupported and the server rejects them.
    serverProperties.setProperty(ServerProperties.Property.ICEBERG_TABLE_ENABLED.getKey(), "true");
  }

  @Override
  protected String tableFormat() {
    return "ICEBERG";
  }

  // These list only the named catalog; the Iceberg suites leave spark_catalog as Spark's built-in
  // session catalog (the UC-connector tests also drive spark_catalog).
  @Override
  protected List<String> supportedCatalogNames() {
    return List.of(CATALOG_NAME);
  }

  @Override
  protected List<String> sessionCatalogNames() {
    return List.of(CATALOG_NAME);
  }

  // Iceberg supports partitioned creates and CTAS, so every create in the base matrix is expected
  // to succeed (the base defaults non-Delta CTAS / non-plain-CREATE to an expected failure).
  @Override
  protected List<String> expectedCreateFailureMessages(TableSetupOptions options) {
    return null;
  }

  // The Iceberg REST catalog vends no cloud credentials and these suites use a local warehouse, so
  // testTableOperations (and the other cloud-parameterized base tests) run once on the file scheme;
  // the base guards its UC-connector-only steps behind !testingIceberg().
  protected static Stream<Arguments> cloudParameters() {
    return Stream.of(Arguments.of("file", false, false));
  }

  /** One column spec for {@link #testTableWithSupportedDataTypes()}. */
  private static final class ColSpec {
    private final String name;
    private final String sqlType;
    private final String insertValue;
    private final String rowValue;
    private final ColumnTypeName typeName;
    // How UC persists the type: typeText is the SQL type string, dataTypeJson the inner Spark
    // DataType JSON (wrapped into the struct-field JSON for the typeJson assertion).
    private final String typeText;
    private final String dataTypeJson;

    private ColSpec(
        String name,
        String sqlType,
        String insertValue,
        String rowValue,
        ColumnTypeName typeName,
        String typeText,
        String dataTypeJson) {
      this.name = name;
      this.sqlType = sqlType;
      this.insertValue = insertValue;
      this.rowValue = rowValue;
      this.typeName = typeName;
      this.typeText = typeText;
      this.dataTypeJson = dataTypeJson;
    }
  }

  /**
   * Reuses the base data-type slot but with an Iceberg-appropriate type set: covers every Iceberg
   * primitive reachable from Spark SQL (int, long, float, double, decimal, string, binary, boolean,
   * date, timestamp, timestamp_ntz) plus the nested types (array, map, struct).
   * Out of scope: Spark's TINYINT / SMALLINT (map to Iceberg int), CHAR / VARCHAR (to string),
   * and TIME / UUID / FIXED / VARIANT (no Spark-SQL-reachable Iceberg equivalent). Asserts both
   * the Spark-visible row values and the column names / type names UC persisted from the Iceberg
   * schema.
   */
  @Override
  @Test
  public void testTableWithSupportedDataTypes() throws ApiException {
    String arrJson = "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true}";
    String mapJson =
        "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\","
            + "\"valueContainsNull\":true}";
    String structJson =
        "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"b\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
    List<ColSpec> cols =
        List.of(
            new ColSpec("c_int", "INT", "1000", "1000", ColumnTypeName.INT, "int", "\"integer\""),
            new ColSpec(
                "c_long", "BIGINT", "100000", "100000", ColumnTypeName.LONG, "bigint", "\"long\""),
            new ColSpec(
                "c_float", "FLOAT", "2.5", "2.5", ColumnTypeName.FLOAT, "float", "\"float\""),
            new ColSpec(
                "c_double", "DOUBLE", "1.5", "1.5", ColumnTypeName.DOUBLE, "double", "\"double\""),
            new ColSpec(
                "c_decimal",
                "DECIMAL(10,2)",
                "123.45",
                "123.45",
                ColumnTypeName.DECIMAL,
                "decimal(10,2)",
                "\"decimal(10,2)\""),
            new ColSpec(
                "c_string", "STRING", "'test'", "test", ColumnTypeName.STRING, "string", "\"string\""),
            // BINARY round-trips as a Java byte[], not a String, so rowValue is unused; the check
            // below compares the raw bytes against the inserted X'CAFEBABE' instead.
            new ColSpec(
                "c_binary",
                "BINARY",
                "X'CAFEBABE'",
                null,
                ColumnTypeName.BINARY,
                "binary",
                "\"binary\""),
            new ColSpec(
                "c_boolean",
                "BOOLEAN",
                "true",
                "true",
                ColumnTypeName.BOOLEAN,
                "boolean",
                "\"boolean\""),
            new ColSpec(
                "c_date",
                "DATE",
                "DATE'2025-01-01'",
                "2025-01-01",
                ColumnTypeName.DATE,
                "date",
                "\"date\""),
            new ColSpec(
                "c_timestamp",
                "TIMESTAMP",
                "TIMESTAMP'2025-01-01 12:00:00'",
                "2025-01-01 12:00:00.0",
                ColumnTypeName.TIMESTAMP,
                "timestamp",
                "\"timestamp\""),
            new ColSpec(
                "c_timestamp_ntz",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ'2025-01-01 12:00:00'",
                "2025-01-01T12:00",
                ColumnTypeName.TIMESTAMP_NTZ,
                "timestamp_ntz",
                "\"timestamp_ntz\""),
            new ColSpec(
                "c_arr",
                "ARRAY<INT>",
                "array(1, 2, 3)",
                "ArraySeq(1, 2, 3)",
                ColumnTypeName.ARRAY,
                "array<int>",
                arrJson),
            new ColSpec(
                "c_map",
                "MAP<STRING, INT>",
                "map('k', 10)",
                "Map(k -> 10)",
                ColumnTypeName.MAP,
                "map<string,int>",
                mapJson),
            new ColSpec(
                "c_struct",
                "STRUCT<a: INT, b: STRING>",
                "struct(42, 'x')",
                "[42,x]",
                ColumnTypeName.STRUCT,
                "struct<a:int,b:string>",
                structJson));

    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String tableName = TEST_TABLE + "_types";
    String fullTableName =
        setupTable(
            new TableSetupOptions()
                .setCatalogName(CATALOG_NAME)
                .setTableName(tableName)
                .setColumns(
                    cols.stream()
                        .map(c -> Pair.of(c.name, c.sqlType))
                        .collect(Collectors.toList())));

    String colNames = cols.stream().map(c -> c.name).collect(Collectors.joining(", "));
    sql(
        "INSERT INTO %s (%s) VALUES (%s)",
        fullTableName,
        colNames,
        cols.stream().map(c -> c.insertValue).collect(Collectors.joining(", ")));

    List<Row> queryResult = sql("SELECT %s FROM %s", colNames, fullTableName);
    assertThat(queryResult).hasSize(1);
    Row row = queryResult.get(0);
    for (int i = 0; i < cols.size(); i++) {
      ColSpec spec = cols.get(i);
      if (spec.typeName == ColumnTypeName.BINARY) {
        assertCafebabeBinary(row.get(i), spec.name);
      } else {
        assertThat(row.get(i).toString())
            .as("row value for %s", spec.name)
            .isEqualTo(spec.rowValue);
      }
    }

    TableInfo tableInfo = tableOperations.getTable(fullTableName);
    // A client-supplied LOCATION registers the table EXTERNAL, otherwise MANAGED; the format is
    // always ICEBERG. Confirms the managed/external subclasses produce what they claim.
    assertThat(tableInfo.getTableType())
        .isEqualTo(isManagedTable() ? TableType.MANAGED : TableType.EXTERNAL);
    assertThat(tableInfo.getDataSourceFormat()).isEqualTo(DataSourceFormat.ICEBERG);
    List<ColumnInfo> columns = tableInfo.getColumns();
    assertThat(columns).hasSize(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      ColumnInfo col = columns.get(i);
      ColSpec spec = cols.get(i);
      assertThat(col.getName()).as("name[%d]", i).isEqualTo(spec.name);
      assertThat(col.getTypeName()).as("typeName for %s", spec.name).isEqualTo(spec.typeName);
      assertThat(col.getTypeText()).as("typeText for %s", spec.name).isEqualTo(spec.typeText);
      assertThat(col.getTypeJson())
          .as("typeJson for %s", spec.name)
          .isEqualTo(structFieldTypeJson(spec.name, spec.dataTypeJson));
    }
  }
}
