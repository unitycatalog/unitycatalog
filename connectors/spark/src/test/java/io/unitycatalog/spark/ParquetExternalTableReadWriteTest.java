package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.TableInfo;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
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

  @Test
  public void testLoadTableKeepsColumnDefaultValue() throws Exception {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);

    String fullTableName =
        setupTable(
            new TableSetupOptions()
                .setCatalogName(CATALOG_NAME)
                .setTableName(TEST_TABLE)
                .setColumns(
                    List.of(
                        Pair.of("a", "INT"),
                        Pair.of("b", "INT DEFAULT 42"),
                        Pair.of("s", "VARCHAR(3) COMMENT 'note'"))));

    Metadata loaded = session.table(fullTableName).schema().apply("b").metadata();
    assertThat(loaded.getString("CURRENT_DEFAULT")).isEqualTo("42");
    assertThat(loaded.getString("EXISTS_DEFAULT")).isEqualTo("42");

    TableCatalog catalog =
        (TableCatalog) session.sessionState().catalogManager().catalog(CATALOG_NAME);
    StructField varcharField =
        catalog
            .loadTable(Identifier.of(new String[] {SCHEMA_NAME}, TEST_TABLE))
            .schema()
            .apply("s");
    assertThat(varcharField.dataType()).isEqualTo(DataType.fromDDL("varchar(3)"));
    assertThat(varcharField.getComment().get()).isEqualTo("note");
    assertThat(varcharField.metadata())
        .isEqualTo(new MetadataBuilder().putString("comment", "note").build());

    sql("INSERT INTO %s VALUES (1, 2, 'abc')", fullTableName);
    assertThatThrownBy(() -> sql("INSERT INTO %s VALUES (2, 2, 'abcd')", fullTableName))
        .hasStackTraceContaining("char/varchar type length limitation");
  }

  @Test
  public void testInsertWithoutDefaultedColumnWritesDefaultValue() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    TableSetupOptions options =
        new TableSetupOptions()
            .setCatalogName(CATALOG_NAME)
            .setTableName(TEST_TABLE)
            .setColumns(List.of(Pair.of("a", "INT"), Pair.of("b", "INT DEFAULT 42")));
    String location = getLocation(options);
    session.sql("SELECT 1 AS a").write().parquet(location);
    session.sql("SELECT 2 AS a, CAST(NULL AS INT) AS b").write().mode("append").parquet(location);
    sql(options.createExternalTableSql(location));
    String fullTableName = options.fullTableName();

    assertThat(sql("SELECT a, b FROM %s ORDER BY a", fullTableName))
        .containsExactly(RowFactory.create(1, 42), RowFactory.create(2, null));

    sql("INSERT INTO %s (a) VALUES (3)", fullTableName);

    assertThat(sql("SELECT a, b FROM %s ORDER BY a", fullTableName))
        .containsExactly(
            RowFactory.create(1, 42), RowFactory.create(2, null), RowFactory.create(3, 42));
  }
}
