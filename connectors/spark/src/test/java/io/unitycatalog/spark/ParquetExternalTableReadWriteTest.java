package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.TableInfo;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class ParquetExternalTableReadWriteTest extends ExternalTableReadWriteTest {
  @Override
  protected String tableFormat() {
    return "PARQUET";
  }

  /**
   * Spark puts the full StructType JSON in {@code spark.sql.sources.schema.part.N}. A seven-column
   * schema is already longer than the server's {@code varchar(255)} property value, so CREATE TABLE
   * used to 500. Schema of record is {@code uc_columns}; those keys must not be persisted.
   */
  @Test
  public void testCreateParquetTableDoesNotPersistSparkDatasourceSchema() throws ApiException {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);

    String fullTableName =
        setupTable(
            new TableSetupOptions()
                .setCatalogName(CATALOG_NAME)
                .setTableName(TEST_TABLE)
                .setColumns(
                    List.of(
                        Pair.of("col1", "STRING"),
                        Pair.of("col2", "INT"),
                        Pair.of("col3", "DOUBLE"),
                        Pair.of("col4", "BIGINT"),
                        Pair.of("col5", "BOOLEAN"),
                        Pair.of("col6", "TIMESTAMP"),
                        Pair.of("col7", "DECIMAL(18, 2)"))));

    TableInfo tableInfo = tableOperations.getTable(fullTableName);
    Map<String, String> serverProperties =
        tableInfo.getProperties() == null ? Map.of() : tableInfo.getProperties();
    assertThat(serverProperties.keySet())
        .noneMatch(
            key ->
                key.equals("spark.sql.sources.schema")
                    || key.startsWith("spark.sql.sources.schema.")
                    || key.equals("spark.sql.partitionSchema")
                    || key.startsWith("spark.sql.partitionSchema."))
        .noneMatch(
            key ->
                key.startsWith(TableCatalog.OPTION_PREFIX + "spark.sql.sources.schema")
                    || key.startsWith(TableCatalog.OPTION_PREFIX + "spark.sql.partitionSchema"));
    assertThat(tableInfo.getColumns()).hasSize(7);

    assertThat(sql("SELECT * FROM %s", fullTableName)).isEmpty();
  }

  /**
   * Dots in column names are legal in Spark but not representable in the {@code catalogString} UC
   * stores as {@code type_text}: a top-level name travels on {@code ColumnInfo.name} alone, and
   * nested field names are left unquoted inside the struct text. Both must come back from {@code
   * type_json} on load -- rebuilding the struct from {@code type_text} fails the analyzer with
   * PARSE_SYNTAX_ERROR at the dot, and a top-level name must not be split on it either.
   */
  @Test
  public void testLoadTableWithDotsInColumnNames() throws ApiException {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);

    String fullTableName =
        setupTable(
            new TableSetupOptions()
                .setCatalogName(CATALOG_NAME)
                .setTableName(TEST_TABLE)
                .setColumns(
                    List.of(
                        Pair.of("`dotted.value`", "STRING"),
                        Pair.of("attrs", "STRUCT<`dotted.name`: STRING, n: INT>"))));

    StructType schema = session.table(fullTableName).schema();
    assertThat(schema.fieldNames()).containsExactly("dotted.value", "attrs");
    assertThat(((StructType) schema.apply("attrs").dataType()).fieldNames())
        .containsExactly("dotted.name", "n");
    assertThat(sql("SELECT `dotted.value`, attrs.`dotted.name` FROM %s", fullTableName)).isEmpty();
  }
}
