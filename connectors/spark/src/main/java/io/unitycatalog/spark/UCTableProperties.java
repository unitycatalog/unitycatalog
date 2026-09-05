package io.unitycatalog.spark;

import java.util.Set;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class UCTableProperties {
  private UCTableProperties() {}

  // This table property should be set to the table ID assigned by UC for managed tables.
  public static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";

  // This table property should be set in order to enable Delta code to use UC as commit coordinator
  public static final String DELTA_CATALOG_MANAGED_KEY = "delta.feature.catalogManaged";
  public static final String DELTA_CATALOG_MANAGED_VALUE = "supported";

  // Spark 4.1 added the `PROP_TABLE_TYPE = "table_type"` constant on `TableCatalog`. We
  // mirror it here so the connector compiles against Spark 4.0 too (where the constant does
  // not exist on the Spark interface). Value must stay in sync with Spark's.
  public static final String PROP_TABLE_TYPE = "table_type";

  // Reserved V2 table properties that are promoted to first-class fields on the UC `CreateTable`
  // payload (provider, location, comment, table_type, ...) and therefore must not
  // be forwarded to the server as part of the `properties` map -- otherwise they would be
  // double-persisted and would not round-trip cleanly on `loadTable`.
  //
  // View-specific fields such as query text and current catalog/namespace are typed fields on
  // Spark's `View`, not TableCatalog properties.
  public static final Set<String> V2_TABLE_PROPERTIES =
      Set.of(
          TableCatalog.PROP_COMMENT,
          TableCatalog.PROP_COLLATION,
          TableCatalog.PROP_EXTERNAL,
          TableCatalog.PROP_IS_MANAGED_LOCATION,
          TableCatalog.PROP_LOCATION,
          TableCatalog.PROP_OWNER,
          TableCatalog.PROP_PROVIDER,
          PROP_TABLE_TYPE);

  // Spark HiveExternalCatalog packages StructType JSON into TBLPROPERTIES so Hive metastore
  // (which has no first-class nested schema) can reconstruct the table. Keys are
  // `spark.sql.sources.schema`, `spark.sql.sources.schema.numParts`,
  // `spark.sql.sources.schema.part.N`, partition/bucket column lists, and the split
  // `spark.sql.partitionSchema*` form. UC already stores schema on `uc_columns`, so these
  // are redundant -- and `schema.part.N` routinely exceeds the server's varchar(255)
  // property value. Strip them on create; do not persist them.
  public static final String SPARK_DATASOURCE_SCHEMA = "spark.sql.sources.schema";
  public static final String SPARK_DATASOURCE_SCHEMA_PREFIX = SPARK_DATASOURCE_SCHEMA + ".";
  public static final String SPARK_PARTITION_SCHEMA = "spark.sql.partitionSchema";
  public static final String SPARK_PARTITION_SCHEMA_PREFIX = SPARK_PARTITION_SCHEMA + ".";

  /**
   * Spark copies USING/OPTIONS entries both bare and with {@link TableCatalog#OPTION_PREFIX}
   * ({@code option.}). Credential and schema keys must match in either form.
   */
  private static String withoutOptionPrefix(String key) {
    if (key.startsWith(TableCatalog.OPTION_PREFIX)) {
      return key.substring(TableCatalog.OPTION_PREFIX.length());
    }
    return key;
  }

  public static boolean isSparkDatasourceSchemaProperty(String key) {
    String bare = withoutOptionPrefix(key);
    return bare.equals(SPARK_DATASOURCE_SCHEMA)
        || bare.startsWith(SPARK_DATASOURCE_SCHEMA_PREFIX)
        || bare.equals(SPARK_PARTITION_SCHEMA)
        || bare.startsWith(SPARK_PARTITION_SCHEMA_PREFIX);
  }

  /**
   * Keys that must not be forwarded on {@code createTable} / {@code createView}. Reserved V2 fields
   * are first-class columns on the UC payload; {@code fs.*} is session-scoped credential config;
   * Spark datasource schema JSON duplicates {@code uc_columns} and overflows varchar(255).
   */
  public static boolean shouldPersistProperty(String key) {
    String bare = withoutOptionPrefix(key);
    if (V2_TABLE_PROPERTIES.contains(key) || V2_TABLE_PROPERTIES.contains(bare)) {
      return false;
    }
    if (bare.startsWith("fs.")) {
      return false;
    }
    return !isSparkDatasourceSchemaProperty(key);
  }
}
