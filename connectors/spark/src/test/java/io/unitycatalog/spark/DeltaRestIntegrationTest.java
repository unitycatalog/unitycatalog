package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end integration test that exercises the Delta REST Catalog API path ({@code
 * deltaRestApi.enabled=true}) against a real Unity Catalog server.
 *
 * <p>This validates Phase 3 (Spark connector via {@link DeltaRestBackend}) end-to-end:
 *
 * <ul>
 *   <li>Table CRUD (create, load, list, drop) through the Delta REST API
 *   <li>Credential vending through the Delta REST API
 *   <li>Managed table staging through the Delta REST API
 *   <li>Data operations (INSERT, SELECT, UPDATE, DELETE) with the new backend
 * </ul>
 *
 * <p>Note: The commit coordinator still uses the legacy API path (published delta-4.1.0 does not
 * include UCDeltaRestClient), so coordinated commits for managed tables go through the legacy
 * DeltaCommitsApi. This tests the coexistence of both API paths.
 */
public class DeltaRestIntegrationTest extends BaseSparkIntegrationTest {

  @TempDir File dataDir;

  /**
   * Overrides session creation to enable the Delta REST Catalog API backend for all catalogs. This
   * is the only difference from the standard test setup - all table operations will be routed
   * through {@link DeltaRestBackend} instead of {@link LegacyUCBackend}.
   */
  @Override
  protected SparkSession createSparkSessionWithCatalogs(
      boolean renewCred, boolean credScopedFsEnabled, String... catalogs) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
    for (String catalog : catalogs) {
      String catalogConf = "spark.sql.catalog." + catalog;
      builder =
          builder
              .config(catalogConf, UCSingleCatalog.class.getName())
              .config(catalogConf + "." + OptionsUtil.URI, serverConfig.getServerUrl())
              .config(catalogConf + "." + OptionsUtil.TOKEN, serverConfig.getAuthToken())
              .config(catalogConf + "." + OptionsUtil.WAREHOUSE, catalog)
              .config(
                  catalogConf + "." + OptionsUtil.RENEW_CREDENTIAL_ENABLED,
                  String.valueOf(renewCred))
              .config(
                  catalogConf + "." + OptionsUtil.CRED_SCOPED_FS_ENABLED,
                  String.valueOf(credScopedFsEnabled))
              .config(catalogConf + "." + OptionsUtil.DELTA_REST_API_ENABLED, "true");
    }
    builder.config("spark.hadoop.fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    builder.config("spark.hadoop.fs.gs.impl", GCSCredentialTestFileSystem.class.getName());
    builder.config("spark.hadoop.fs.abfs.impl", AzureCredentialTestFileSystem.class.getName());
    return builder.getOrCreate();
  }

  @Test
  public void testExternalDeltaTableCrud() throws IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String location = new File(dataDir, "ext_delta").getCanonicalPath();
    String fullTableName = CATALOG_NAME + "." + SCHEMA_NAME + ".test_ext_delta";

    // CREATE external Delta table through Delta REST API
    sql("CREATE TABLE %s (i INT, s STRING) USING DELTA LOCATION '%s'", fullTableName, location);

    // INSERT data (Delta handles commits internally for external tables)
    sql("INSERT INTO %s SELECT 1, 'hello'", fullTableName);

    // SELECT through Delta REST API (loadTable + credential vending)
    List<Row> rows = sql("SELECT * FROM %s", fullTableName);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows.get(0).getString(1)).isEqualTo("hello");

    // Additional INSERT
    sql("INSERT INTO %s SELECT 2, 'world'", fullTableName);
    rows = sql("SELECT * FROM %s ORDER BY i", fullTableName);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows.get(1).getInt(0)).isEqualTo(2);

    // UPDATE (Delta-specific)
    sql("UPDATE %s SET s = 'updated' WHERE i = 1", fullTableName);
    rows = sql("SELECT * FROM %s WHERE i = 1", fullTableName);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getString(1)).isEqualTo("updated");

    // DELETE (Delta-specific)
    sql("DELETE FROM %s WHERE i = 2", fullTableName);
    rows = sql("SELECT * FROM %s", fullTableName);
    assertThat(rows).hasSize(1);

    // SHOW TABLES through Delta REST API (listTables)
    List<Row> tables = sql("SHOW TABLES IN %s.%s", CATALOG_NAME, SCHEMA_NAME);
    assertThat(tables).hasSize(1);
    assertThat(tables.get(0).getString(1)).isEqualTo("test_ext_delta");

    // DROP TABLE through Delta REST API
    assertThat(session.catalog().tableExists(fullTableName)).isTrue();
    sql("DROP TABLE %s", fullTableName);
    assertThat(session.catalog().tableExists(fullTableName)).isFalse();
  }

  @Test
  public void testManagedDeltaTableCreateAndRead() throws ApiException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    sql("CREATE DATABASE IF NOT EXISTS spark_catalog.%s", SCHEMA_NAME);

    String fullTableName = CATALOG_NAME + "." + SCHEMA_NAME + ".test_managed_delta";

    // CREATE managed table through Delta REST API (staging + create)
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING DELTA TBLPROPERTIES ('%s'='%s')",
        fullTableName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);

    // INSERT (commits go through commit coordinator - legacy path in published delta)
    sql("INSERT INTO %s SELECT 1, 'managed'", fullTableName);

    // SELECT through Delta REST API
    List<Row> rows = sql("SELECT * FROM %s", fullTableName);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows.get(0).getString(1)).isEqualTo("managed");

    // Verify table metadata through legacy UC API (server stores the same data)
    TableOperations tableOperations = new SdkTableOperations(createApiClient(serverConfig));
    TableInfo tableInfo = tableOperations.getTable(fullTableName);
    assertThat(tableInfo.getTableType()).isEqualTo(TableType.MANAGED);
    assertThat(tableInfo.getDataSourceFormat()).isEqualTo(DataSourceFormat.DELTA);

    // Verify managed table properties
    Map<String, String> props = tableInfo.getProperties();
    assertThat(props).containsKey(UCTableProperties.UC_TABLE_ID_KEY);
    assertThat(props)
        .containsEntry(
            UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
            UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);

    // Verify the protocol was correctly stored by the server.
    // The DeltaRestBackend extracts protocol from properties and sends it as a structured
    // object. The server applies it back to properties. Verify the round-trip is correct.
    assertThat(props).containsKey("delta.minReaderVersion");
    assertThat(props).containsKey("delta.minWriterVersion");
    int minReader = Integer.parseInt(props.get("delta.minReaderVersion"));
    int minWriter = Integer.parseInt(props.get("delta.minWriterVersion"));
    // catalogManaged tables require table features protocol (reader v3, writer v7)
    assertThat(minReader).isGreaterThanOrEqualTo(3);
    assertThat(minWriter).isGreaterThanOrEqualTo(7);
    // Verify required features were stored
    assertThat(props).containsEntry("delta.feature.catalogManaged", "supported");

    // INSERT more data and verify
    sql("INSERT INTO %s SELECT 2, 'also_managed'", fullTableName);
    rows = sql("SELECT * FROM %s ORDER BY i", fullTableName);
    assertThat(rows).hasSize(2);
  }

  @Test
  public void testListMultipleTables() throws IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    // Create multiple external tables
    for (int i = 0; i < 3; i++) {
      String location = new File(dataDir, "list_table_" + i).getCanonicalPath();
      sql(
          "CREATE TABLE %s.%s.list_table_%d (id INT) USING DELTA LOCATION '%s'",
          CATALOG_NAME, SCHEMA_NAME, i, location);
    }

    // SHOW TABLES through Delta REST API
    List<Row> tables = sql("SHOW TABLES IN %s.%s", CATALOG_NAME, SCHEMA_NAME);
    assertThat(tables).hasSize(3);
    List<String> tableNames =
        tables.stream().map(row -> row.getString(1)).sorted().collect(Collectors.toList());
    assertThat(tableNames).containsExactly("list_table_0", "list_table_1", "list_table_2");

    // Clean up
    for (int i = 0; i < 3; i++) {
      sql("DROP TABLE %s.%s.list_table_%d", CATALOG_NAME, SCHEMA_NAME, i);
    }

    // Verify all dropped
    tables = sql("SHOW TABLES IN %s.%s", CATALOG_NAME, SCHEMA_NAME);
    assertThat(tables).isEmpty();
  }

  @Test
  public void testPartitionedExternalTable() throws IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String location = new File(dataDir, "part_delta").getCanonicalPath();
    String fullTableName = CATALOG_NAME + "." + SCHEMA_NAME + ".test_partitioned";

    // CREATE partitioned table
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING DELTA PARTITIONED BY (s) LOCATION '%s'",
        fullTableName, location);

    // INSERT data into different partitions
    sql("INSERT INTO %s SELECT 1, 'a'", fullTableName);
    sql("INSERT INTO %s SELECT 2, 'b'", fullTableName);
    sql("INSERT INTO %s SELECT 3, 'a'", fullTableName);

    // SELECT all
    List<Row> rows = sql("SELECT * FROM %s ORDER BY i", fullTableName);
    assertThat(rows).hasSize(3);

    // Partition pruning query
    rows = sql("SELECT * FROM %s WHERE s = 'a' ORDER BY i", fullTableName);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows.get(1).getInt(0)).isEqualTo(3);

    sql("DROP TABLE %s", fullTableName);
  }

  @Test
  public void testTimeTravelExternalTable() throws IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String location = new File(dataDir, "tt_delta").getCanonicalPath();
    String fullTableName = SPARK_CATALOG + "." + SCHEMA_NAME + ".test_timetravel";

    sql("CREATE TABLE %s (i INT, s STRING) USING DELTA LOCATION '%s'", fullTableName, location);
    sql("INSERT INTO %s SELECT 1, 'v1'", fullTableName);
    sql("INSERT INTO %s SELECT 2, 'v2'", fullTableName);

    // Version 1 should have 1 row, version 2 should have 2 rows
    List<Row> v1Rows = sql("SELECT * FROM %s VERSION AS OF 1", fullTableName);
    assertThat(v1Rows).hasSize(1);
    assertThat(v1Rows.get(0).getInt(0)).isEqualTo(1);

    List<Row> v2Rows = sql("SELECT * FROM %s VERSION AS OF 2", fullTableName);
    assertThat(v2Rows).hasSize(2);

    sql("DROP TABLE %s", fullTableName);
  }

  @Test
  public void testMergeIntoExternalTable() throws IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String loc1 = new File(dataDir, "merge_target").getCanonicalPath();
    String loc2 = new File(dataDir, "merge_source").getCanonicalPath();
    String target = CATALOG_NAME + "." + SCHEMA_NAME + ".merge_target";
    String source = CATALOG_NAME + "." + SCHEMA_NAME + ".merge_source";

    // Create target and source tables
    sql("CREATE TABLE %s (i INT, s STRING) USING DELTA LOCATION '%s'", target, loc1);
    sql("CREATE TABLE %s (i INT, s STRING) USING DELTA LOCATION '%s'", source, loc2);

    sql("INSERT INTO %s SELECT 1, 'old'", target);
    sql("INSERT INTO %s SELECT 1, 'new'", source);
    sql("INSERT INTO %s SELECT 2, 'added'", source);

    // MERGE INTO
    sql(
        "MERGE INTO %s t USING %s s ON t.i = s.i "
            + "WHEN MATCHED THEN UPDATE SET s = s.s "
            + "WHEN NOT MATCHED THEN INSERT *",
        target, source);

    List<Row> rows = sql("SELECT * FROM %s ORDER BY i", target);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows.get(0).getString(1)).isEqualTo("new");
    assertThat(rows.get(1).getInt(0)).isEqualTo(2);
    assertThat(rows.get(1).getString(1)).isEqualTo("added");

    sql("DROP TABLE %s", target);
    sql("DROP TABLE %s", source);
  }
}
