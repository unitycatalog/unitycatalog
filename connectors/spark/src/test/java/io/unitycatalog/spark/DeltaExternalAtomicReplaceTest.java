package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DeltaExternalAtomicReplaceTest extends BaseSparkIntegrationTest {

  private static final String TEST_TABLE = "external_delta_table";
  private static final String ANOTHER_TEST_TABLE = "external_delta_table_other";
  private static final String EXTERNAL_TABLE_COMMENT = "existing external delta table";

  @TempDir protected File dataDir;

  private TableOperations tableOperations;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    tableOperations = new SdkTableOperations(createApiClient(serverConfig));
  }

  @Test
  public void testReplaceExternalDeltaTablePreservesUcIdentityAndLocation()
      throws IOException, ApiException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String fullTableName = createExternalDeltaTable(TEST_TABLE, EXTERNAL_TABLE_COMMENT, false);
    sql("INSERT INTO %s VALUES (1, 'a')", fullTableName);

    TableInfo tableInfoBeforeReplace = tableOperations.getTable(fullTableName);
    String expectedTableId = tableInfoBeforeReplace.getTableId();
    String expectedLocation = tableInfoBeforeReplace.getStorageLocation();
    long versionBeforeReplace = latestDeltaVersion(fullTableName);

    sql("REPLACE TABLE %s (i INT, s STRING) USING DELTA", fullTableName);
    validateTableEmpty(fullTableName);
    assertThat(latestDeltaVersion(fullTableName)).isEqualTo(versionBeforeReplace + 1);
    assertExternalDeltaTableInfo(
        tableOperations.getTable(fullTableName),
        expectedTableId,
        expectedLocation,
        EXTERNAL_TABLE_COMMENT);

    sql("INSERT INTO %s VALUES (2, 'b')", fullTableName);
    long versionBeforeReplaceAsSelect = latestDeltaVersion(fullTableName);
    sql("REPLACE TABLE %s USING DELTA AS SELECT 3 AS i, 'c' AS s", fullTableName);
    validateRows(sql("SELECT * FROM %s", fullTableName), Pair.of(3, "c"));
    assertThat(latestDeltaVersion(fullTableName)).isEqualTo(versionBeforeReplaceAsSelect + 1);
    assertExternalDeltaTableInfo(
        tableOperations.getTable(fullTableName),
        expectedTableId,
        expectedLocation,
        EXTERNAL_TABLE_COMMENT);
  }

  @Test
  public void testCreateOrReplaceExternalDeltaTablePreservesUcIdentityAndLocation()
      throws IOException, ApiException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String fullTableName = createExternalDeltaTable(TEST_TABLE, EXTERNAL_TABLE_COMMENT, false);
    sql("INSERT INTO %s VALUES (1, 'a')", fullTableName);

    TableInfo tableInfoBeforeReplace = tableOperations.getTable(fullTableName);
    String expectedTableId = tableInfoBeforeReplace.getTableId();
    String expectedLocation = tableInfoBeforeReplace.getStorageLocation();
    long versionBeforeCreateOrReplace = latestDeltaVersion(fullTableName);

    sql("CREATE OR REPLACE TABLE %s (i INT, s STRING) USING DELTA", fullTableName);
    validateTableEmpty(fullTableName);
    assertThat(latestDeltaVersion(fullTableName)).isEqualTo(versionBeforeCreateOrReplace + 1);
    assertExternalDeltaTableInfo(
        tableOperations.getTable(fullTableName),
        expectedTableId,
        expectedLocation,
        EXTERNAL_TABLE_COMMENT);

    sql("INSERT INTO %s VALUES (2, 'b')", fullTableName);
    long versionBeforeCreateOrReplaceAsSelect = latestDeltaVersion(fullTableName);
    sql("CREATE OR REPLACE TABLE %s USING DELTA AS SELECT 4 AS i, 'd' AS s", fullTableName);
    validateRows(sql("SELECT * FROM %s", fullTableName), Pair.of(4, "d"));
    assertThat(latestDeltaVersion(fullTableName))
        .isEqualTo(versionBeforeCreateOrReplaceAsSelect + 1);
    assertExternalDeltaTableInfo(
        tableOperations.getTable(fullTableName),
        expectedTableId,
        expectedLocation,
        EXTERNAL_TABLE_COMMENT);
  }

  @Test
  public void testCreateOrReplaceExternalDeltaTableCreatesWhenMissing()
      throws IOException, ApiException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String fullTableName = fullTableName(TEST_TABLE);
    String location = locationFor(TEST_TABLE);

    sql(
        "CREATE OR REPLACE TABLE %s (i INT, s STRING) USING DELTA LOCATION '%s'",
        fullTableName, location);
    validateTableEmpty(fullTableName);

    TableInfo tableInfo = tableOperations.getTable(fullTableName);
    assertThat(tableInfo.getTableId()).isNotBlank();
    assertExternalDeltaTableInfo(tableInfo, tableInfo.getTableId(), location, null);
  }

  @Test
  public void testCreateOrReplaceExternalDeltaTableAsSelectCreatesWhenMissing()
      throws IOException, ApiException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String fullTableName = fullTableName(TEST_TABLE);
    String location = locationFor(TEST_TABLE);

    sql(
        "CREATE OR REPLACE TABLE %s USING DELTA LOCATION '%s' AS SELECT 2 AS i, 'b' AS s",
        fullTableName, location);
    validateRows(sql("SELECT * FROM %s", fullTableName), Pair.of(2, "b"));

    TableInfo tableInfo = tableOperations.getTable(fullTableName);
    assertThat(tableInfo.getTableId()).isNotBlank();
    assertExternalDeltaTableInfo(tableInfo, tableInfo.getTableId(), location, null);
  }

  @Test
  public void testExternalDeltaReplaceRejectsMetadataChanges() throws IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String fullTableName = createExternalDeltaTable(TEST_TABLE, EXTERNAL_TABLE_COMMENT, true);
    String differentLocation = locationFor(ANOTHER_TEST_TABLE);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", fullTableName);
    long versionBefore = latestDeltaVersion(fullTableName);

    List<String> statements =
        List.of(
            String.format("REPLACE TABLE %s (i INT, renamed STRING) USING DELTA", fullTableName),
            String.format(
                "REPLACE TABLE %s (i INT, s STRING) USING DELTA PARTITIONED BY (i)", fullTableName),
            String.format(
                "REPLACE TABLE %s (i INT, s STRING) USING DELTA COMMENT 'new comment'",
                fullTableName),
            String.format(
                "CREATE OR REPLACE TABLE %s (i INT, s STRING) USING DELTA "
                    + "TBLPROPERTIES ('delta.appendOnly' = 'true')",
                fullTableName),
            String.format(
                "CREATE OR REPLACE TABLE %s (i INT, s STRING) USING DELTA LOCATION '%s'",
                fullTableName, differentLocation));

    for (String statement : statements) {
      assertThatThrownBy(() -> sql(statement)).hasMessageContaining("only supports data refresh");
    }

    assertThatThrownBy(
            () ->
                sql("REPLACE TABLE %s USING DELTA AS SELECT 1 AS i, 2 AS extra_col", fullTableName))
        .hasMessageContaining("only supports data refresh");
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE OR REPLACE TABLE %s USING DELTA AS SELECT 1 AS i, 2 AS extra_col",
                    fullTableName))
        .hasMessageContaining("only supports data refresh");

    assertThat(latestDeltaVersion(fullTableName)).isEqualTo(versionBefore);
    validateRows(
        sql("SELECT * FROM %s ORDER BY i", fullTableName), Pair.of(1, "a"), Pair.of(2, "b"));
  }

  @Test
  public void testExternalDeltaReplaceFailuresPreserveExistingDataAndVersion()
      throws IOException, ApiException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String fullTableName = createExternalDeltaTable(TEST_TABLE, EXTERNAL_TABLE_COMMENT, false);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", fullTableName);

    TableInfo tableInfoBeforeFailure = tableOperations.getTable(fullTableName);
    long versionBeforeRtasFailure = latestDeltaVersion(fullTableName);
    assertThatThrownBy(
            () ->
                sql(
                    "REPLACE TABLE %s USING DELTA AS "
                        + "SELECT i, IF(i = 2, CAST(raise_error('boom') AS STRING), s) AS s "
                        + "FROM %s",
                    fullTableName, fullTableName))
        .isInstanceOf(Exception.class);
    assertThat(latestDeltaVersion(fullTableName)).isEqualTo(versionBeforeRtasFailure);
    validateRows(
        sql("SELECT * FROM %s ORDER BY i", fullTableName), Pair.of(1, "a"), Pair.of(2, "b"));
    assertExternalDeltaTableInfo(
        tableOperations.getTable(fullTableName),
        tableInfoBeforeFailure.getTableId(),
        tableInfoBeforeFailure.getStorageLocation(),
        EXTERNAL_TABLE_COMMENT);

    long versionBeforeCtorFailure = latestDeltaVersion(fullTableName);
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE OR REPLACE TABLE %s USING DELTA AS "
                        + "SELECT i, IF(i = 2, CAST(raise_error('boom') AS STRING), s) AS s "
                        + "FROM %s",
                    fullTableName, fullTableName))
        .isInstanceOf(Exception.class);
    assertThat(latestDeltaVersion(fullTableName)).isEqualTo(versionBeforeCtorFailure);
    validateRows(
        sql("SELECT * FROM %s ORDER BY i", fullTableName), Pair.of(1, "a"), Pair.of(2, "b"));
    assertExternalDeltaTableInfo(
        tableOperations.getTable(fullTableName),
        tableInfoBeforeFailure.getTableId(),
        tableInfoBeforeFailure.getStorageLocation(),
        EXTERNAL_TABLE_COMMENT);
  }

  @Test
  public void testReplaceNonDeltaExternalTableRejected() throws IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String fullTableName = fullTableName(TEST_TABLE);
    String location = locationFor(TEST_TABLE);
    sql("CREATE TABLE %s (i INT, s STRING) USING PARQUET LOCATION '%s'", fullTableName, location);

    assertThatThrownBy(() -> sql("REPLACE TABLE %s (i INT, s STRING) USING DELTA", fullTableName))
        .hasMessageContaining("REPLACE TABLE is only supported for UC Delta tables");
  }

  private String createExternalDeltaTable(String tableName, String comment, boolean partitioned)
      throws IOException {
    String fullTableName = fullTableName(tableName);
    String location = locationFor(tableName);
    String partitionClause = partitioned ? "PARTITIONED BY (s)" : "";
    String commentClause = comment != null ? String.format("COMMENT '%s'", comment) : "";
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING DELTA %s %s LOCATION '%s'",
        fullTableName, partitionClause, commentClause, location);
    return fullTableName;
  }

  private String fullTableName(String tableName) {
    return String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, tableName);
  }

  private String locationFor(String tableName) throws IOException {
    return new File(new File(dataDir, CATALOG_NAME), tableName).getCanonicalPath();
  }

  private String canonicalizeLocation(String location) {
    try {
      if (location.contains("://") || location.startsWith("file:/")) {
        URI uri = URI.create(location).normalize();
        if ("file".equalsIgnoreCase(uri.getScheme())) {
          return new File(uri).getCanonicalPath();
        }
        return stripTrailingSlash(uri.toString());
      }
      return new File(location).getCanonicalPath();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to canonicalize location: " + location, e);
    }
  }

  private String stripTrailingSlash(String location) {
    if (location.endsWith("/")) {
      return location.substring(0, location.length() - 1);
    }
    return location;
  }

  private void validateRows(List<Row> rows, Pair<Integer, String>... expected) {
    assertThat(rows).hasSize(expected.length);
    for (int i = 0; i < expected.length; i++) {
      Row row = rows.get(i);
      assertThat(row.getInt(0)).isEqualTo(expected[i].getLeft());
      assertThat(row.getString(1)).isEqualTo(expected[i].getRight());
    }
  }

  private void validateTableEmpty(String fullTableName) {
    assertThat(session.table(fullTableName).collectAsList()).isEmpty();
  }

  private void assertExternalDeltaTableInfo(
      TableInfo tableInfo,
      String expectedTableId,
      String expectedLocation,
      String expectedComment) {
    assertThat(tableInfo.getTableId()).isEqualTo(expectedTableId);
    assertThat(canonicalizeLocation(tableInfo.getStorageLocation()))
        .isEqualTo(canonicalizeLocation(expectedLocation));
    assertThat(tableInfo.getTableType()).isEqualTo(TableType.EXTERNAL);
    assertThat(tableInfo.getDataSourceFormat()).isEqualTo(DataSourceFormat.DELTA);
    assertThat(tableInfo.getComment()).isEqualTo(expectedComment);
  }
}
