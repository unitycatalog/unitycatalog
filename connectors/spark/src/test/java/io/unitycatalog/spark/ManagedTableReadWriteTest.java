package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ManagedTableReadWriteTest extends BaseTableReadWriteTest {
  private static final String DELTA_TABLE = "test_delta";

  /**
   * This function provides a set of test parameters that cloud-aware tests should run for this
   * class.
   *
   * @return A stream of Arguments.of(String scheme, boolean renewCredEnabled)
   */
  protected static Stream<Arguments> cloudParameters() {
    // Right now this test suite only support testing with s3. In the future we'll expand this
    // test to all supported clouds when UC supports setting up multiple managed storage locations.
    return Stream.of(Arguments.of("s3", false), Arguments.of("s3", true));
  }

  @Override
  protected String managedStorageCloudScheme() {
    return "s3";
  }

  @Test
  public void testCreateManagedTableErrors() throws IOException {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String fullTableName = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    assertThatThrownBy(() -> sql("CREATE TABLE %s(name STRING) USING parquet", fullTableName))
        .hasMessageContaining("not support non-Delta managed table");
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta "
                        + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'disabled')",
                    fullTableName))
        .hasMessageContaining("delta.feature.catalogOwned-preview=disabled");
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta "
                        + "TBLPROPERTIES ('ucTableId' = 'some_id')",
                    fullTableName))
        .hasMessageContaining("ucTableId");
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta "
                        + "TBLPROPERTIES ('is_managed_location' = 'false')",
                    fullTableName))
        .hasMessageContaining("is_managed_location");
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testCreateManagedDeltaTable(String scheme, boolean renewCredEnabled)
      throws IOException, URISyntaxException {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG, CATALOG_NAME);

    int counter = 0;
    for (boolean setProperty : List.of(true, false)) {
      for (boolean ctas : List.of(true, false)) {
        for (String catalogName : List.of(SPARK_CATALOG, CATALOG_NAME)) {
          String fullTableName = catalogName + "." + SCHEMA_NAME + "." + DELTA_TABLE + counter;
          counter++;
          // Setting this table property isn't necessary, but we should not throw an error.
          String propertyClause =
              setProperty
                  ? "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')"
                  : "";

          if (ctas) {
            sql(
                "CREATE TABLE %s USING delta %s AS SELECT 'a' AS name",
                fullTableName, propertyClause);
          } else {
            sql("CREATE TABLE %s(name STRING) USING delta %s", fullTableName, propertyClause);
          }
          sql("INSERT INTO " + fullTableName + " SELECT 'b'");

          List<Row> rows = sql("DESC EXTENDED " + fullTableName);
          Map<String, String> describeResult =
              rows.stream()
                  .collect(Collectors.toMap(row -> row.getString(0), row -> row.getString(1)));

          // Make sure the table created is managed and catalogOwned
          assertThat(describeResult.get("Name")).isEqualTo(fullTableName);
          assertThat(describeResult.get("Type")).isEqualTo("MANAGED");
          assertThat(describeResult.get("Provider")).isEqualToIgnoringCase("delta");
          assertThat(describeResult.get("Is_managed_location")).isEqualTo("true");
          assertThat(describeResult).containsKey("Table Properties");
          assertThat(describeResult.get("Table Properties"))
              .contains("delta.feature.catalogOwned-preview=supported");
          assertThat(describeResult.get("Table Properties")).contains("ucTableId=");

          // Make sure the table has commit under /_delta_log/_staged_commits
          assertThat(describeResult).containsKey("Location");
          String location = describeResult.get("Location");
          URI uri = new URI(location);
          assertThat(uri.getScheme()).isEqualTo(scheme);
          String path = uri.getPath();
          String stagedCommitPath = path + "/_delta_log/_staged_commits";
          try (DirectoryStream<Path> directoryStream =
              Files.newDirectoryStream(Path.of(stagedCommitPath))) {
            Set<String> jsonFiles =
                StreamSupport.stream(directoryStream.spliterator(), false)
                    .map(Path::toString)
                    .filter(s -> s.endsWith(".json"))
                    .collect(Collectors.toSet());
            assertThat(jsonFiles).hasSizeGreaterThanOrEqualTo(1);
          }
        }
      }
    }
  }

  @Override
  protected String setupDeltaTable(
      String cloudScheme, String catalogName, String tableName, List<String> partitionColumns)
      throws IOException, ApiException {
    // For now, we only support testing one cloud, which is the one configured by
    // managedStorageCloudScheme(). Tests are only supposed to call this function with the correct
    // cloud scheme.
    assert cloudScheme.equals(managedStorageCloudScheme());
    String partitionClause;
    if (partitionColumns.isEmpty()) {
      partitionClause = "";
    } else {
      partitionClause = String.format(" PARTITIONED BY (%s)", String.join(", ", partitionColumns));
    }
    sql(
        "CREATE TABLE %s.%s.%s(i INT, s STRING) USING delta %s",
        catalogName, SCHEMA_NAME, tableName, partitionClause);
    return String.join(".", catalogName, SCHEMA_NAME, tableName);
  }
}
