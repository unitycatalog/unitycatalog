package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Verifies end-to-end schema/type mapping between Spark and Unity Catalog, including nested
 * structures.
 */
public class FieldsMappingTest extends BaseSparkIntegrationTest {

  private static final String SCHEMA_NAME = "my_test_database";
  private static final String TABLE_NAME = "tableName";

  @Test
  public void testAllSparkDataTypesAreMappedCorrectlyFromUnityCatalog() {
    try (ExternalTablesManager externalTablesManager = new ExternalTablesManager()) {
      // Given: Spark session configured to sync schema changes to Unity Catalog
      session =
          createSparkSessionWithCatalogs(
              Map.of(DELTA_UPDATE_CATALOG_ENABLED_PROPERTY, "true"), SPARK_CATALOG, CATALOG_NAME);
      session.catalog().setCurrentCatalog(CATALOG_NAME);

      sql(String.format("CREATE DATABASE %s;", SCHEMA_NAME));

      String fullTableName = String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, TABLE_NAME);

      // When: Creating an external Delta table with comprehensive data types
      sql(
          String.format(
              "CREATE EXTERNAL TABLE %s ("
                  // Numeric types
                  + "col_byte BYTE, "
                  + "col_short SHORT, "
                  + "col_int INT, "
                  + "col_long LONG, "
                  + "col_float FLOAT, "
                  + "col_double DOUBLE, "
                  + "col_decimal DECIMAL(10, 2), "
                  // String type
                  + "col_string STRING, "
                  + "col_varchar VARCHAR(100), "
                  + "col_char CHAR(10), "
                  // Binary type
                  + "col_binary BINARY, "
                  // Boolean type
                  + "col_boolean BOOLEAN, "
                  // Datetime type
                  + "col_date DATE, "
                  + "col_timestamp TIMESTAMP, "
                  + "col_timestamp_ntz TIMESTAMP_NTZ, "
                  // Complex types
                  + "col_array ARRAY<STRING>, "
                  + "col_map MAP<STRING, INT>, "
                  + "col_struct STRUCT<field1: STRING, field2: INT>, "
                  + "col_nested_struct STRUCT<nested_field: STRUCT<deep_field: STRING>>, "
                  + "col_array_of_struct ARRAY<STRUCT<field1: STRING, field2: INT>>, "
                  + "col_map_of_array MAP<STRING, ARRAY<INT>>"
                  + ") USING DELTA LOCATION '%s'",
              fullTableName, externalTablesManager.getLocation(fullTableName)));

      StructType loadedTableSchema =
          loadTableFromCatalog(CATALOG_NAME, SCHEMA_NAME, TABLE_NAME).schema();

      Metadata nullComment = Metadata.fromJson("{\"comment\":null}");
      StructType expectedSchema =
          new StructType()
              // Numeric types
              .add("col_byte", DataTypes.ByteType, true, nullComment)
              .add("col_short", DataTypes.ShortType, true, nullComment)
              .add("col_int", DataTypes.IntegerType, true, nullComment)
              .add("col_long", DataTypes.LongType, true, nullComment)
              .add("col_float", DataTypes.FloatType, true, nullComment)
              .add("col_double", DataTypes.DoubleType, true, nullComment)
              .add("col_decimal", DataTypes.createDecimalType(10, 2), true, nullComment)
              // String type
              .add("col_string", DataTypes.StringType, true, nullComment)
              .add("col_varchar", DataTypes.StringType, true, nullComment)
              .add("col_char", DataTypes.StringType, true, nullComment)
              // Binary type
              .add("col_binary", DataTypes.BinaryType, true, nullComment)
              // Boolean type
              .add("col_boolean", DataTypes.BooleanType, true, nullComment)
              // Datetime type
              .add("col_date", DataTypes.DateType, true, nullComment)
              .add("col_timestamp", DataTypes.TimestampType, true, nullComment)
              .add("col_timestamp_ntz", DataTypes.TimestampNTZType, true, nullComment)
              // Complex types
              .add(
                  "col_array",
                  DataTypes.createArrayType(DataTypes.StringType, true),
                  true,
                  nullComment)
              .add(
                  "col_map",
                  DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true),
                  true,
                  nullComment)
              .add(
                  "col_struct",
                  new StructType()
                      .add("field1", DataTypes.StringType, true)
                      .add("field2", DataTypes.IntegerType, true),
                  true,
                  nullComment)
              .add(
                  "col_nested_struct",
                  new StructType()
                      .add(
                          "nested_field",
                          new StructType().add("deep_field", DataTypes.StringType, true),
                          true),
                  true,
                  nullComment)
              .add(
                  "col_array_of_struct",
                  DataTypes.createArrayType(
                      new StructType()
                          .add("field1", DataTypes.StringType, true)
                          .add("field2", DataTypes.IntegerType, true),
                      true),
                  true,
                  nullComment)
              .add(
                  "col_map_of_array",
                  DataTypes.createMapType(
                      DataTypes.StringType,
                      DataTypes.createArrayType(DataTypes.IntegerType, true),
                      true),
                  true,
                  nullComment);

      assertThat(loadedTableSchema).isEqualTo(expectedSchema);
    }
  }

  /**
   * Loads a table from the Unity Catalog using the provided catalog, schema, and table names. It
   * loads the table in as raw form as possible. Other methods introduce unwanted modifications like
   * schema comment updates.
   */
  private CatalogTable loadTableFromCatalog(
      String catalogName, String schemaName, String tableName) {
    UCSingleCatalog catalog =
        (UCSingleCatalog) session.sessionState().catalogManager().catalog(catalogName);
    DeltaTableV2 table =
        (DeltaTableV2) catalog.loadTable(Identifier.of(new String[] {schemaName}, tableName));
    return table.catalogTable().get();
  }
}
