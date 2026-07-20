package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.spark.utils.OptionsUtil;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for {@link ResolvePathCredentials}: Unity Catalog credentials are vended for cloud paths
 * referenced directly in a query (e.g. {@code parquet.`s3://bucket/dir`}), without a pre-registered
 * external table.
 *
 * <p>Unlike the other Spark integration tests, these register {@code UCSparkSessionExtensions} (the
 * home of the parser hook that invokes the rule), and use the {@link S3CredentialTestFileSystem}
 * fake filesystem to assert the vended credentials reach S3A. The test principal is the metastore
 * owner, so path authorization passes and credentials fall back to the per-bucket server config
 * ({@code accessKey0}/... for {@code s3://test-bucket0}) configured in {@link
 * BaseSparkIntegrationTest#setUpProperties()}.
 */
public class PathCredentialReadWriteTest extends BaseSparkIntegrationTest {

  @TempDir protected File dataDir;

  /**
   * Builds a Spark session that registers {@code UCSparkSessionExtensions} (so the parser-level
   * {@link ResolvePathCredentials} hook is active) and points the given catalogs at the test UC
   * server. Mirrors {@link BaseSparkIntegrationTest#createSparkSessionWithCatalogs} but adds the UC
   * extension. The catalogs are expected to already exist (created in {@code setUp}).
   */
  private SparkSession createUcSparkSession(
      boolean renewCred,
      boolean credScopedFsEnabled,
      boolean vendPathCredentialsEnabled,
      String... catalogs) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension,"
                    + "io.unitycatalog.spark.UCSparkSessionExtensions");
    for (String catalog : catalogs) {
      String catalogConf = "spark.sql.catalog." + catalog;
      builder =
          builder
              .config(catalogConf, UCSingleCatalog.class.getName())
              .config(catalogConf + "." + OptionsUtil.URI, serverConfig.getServerUrl())
              .config(catalogConf + "." + OptionsUtil.TOKEN, serverConfig.getAuthToken())
              .config(catalogConf + "." + OptionsUtil.WAREHOUSE, catalog)
              .config(catalogConf + "." + OptionsUtil.RENEW_CREDENTIAL_ENABLED, renewCred)
              .config(catalogConf + "." + OptionsUtil.CRED_SCOPED_FS_ENABLED, credScopedFsEnabled)
              .config(
                  catalogConf + "." + OptionsUtil.VEND_PATH_CREDENTIALS_ENABLED,
                  vendPathCredentialsEnabled);
    }
    // Use fake file system for cloud storage so that we can assert vended credentials.
    builder.config("spark.hadoop.fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    return builder.getOrCreate();
  }

  /** A bare `s3://test-bucket0/...` path backed by a local temp dir understood by the fake FS. */
  private String bucketPath(String name) throws IOException {
    return "s3://test-bucket0" + new File(dataDir, name).getCanonicalPath();
  }

  private void stopSession() {
    if (session != null) {
      session.stop();
      session = null;
    }
  }

  private void assertSingleRow(List<Row> rows) {
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows.get(0).getString(1)).isEqualTo("a");
  }

  /**
   * Writes to a bare cloud path and reads it back. Exercises {@code INSERT OVERWRITE DIRECTORY}
   * ({@code InsertIntoDir}) and {@code parquet.`path`} reads ({@code UnresolvedRelation}).
   * Bare-path {@code INSERT INTO} write targets are covered at parse time in {@link
   * #testInsertIntoBarePathInjectsCredentialsAtParseTime()} because Spark 4.1 rejects them at
   * analysis ({@code TABLE_OR_VIEW_NOT_FOUND}).
   */
  @ParameterizedTest
  @CsvSource({"false, false", "true, true"})
  public void testWriteAndReadBareS3Path(boolean renewCred, boolean credScopedFsEnabled)
      throws IOException {
    stopSession();
    session = createUcSparkSession(renewCred, credScopedFsEnabled, true, SPARK_CATALOG);
    String location = bucketPath("write_directory_" + renewCred + "_" + credScopedFsEnabled);

    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);
    assertSingleRow(sql("SELECT * FROM parquet.`%s`", location));
  }

  /**
   * Spark 4.1 rejects {@code INSERT INTO parquet.`s3://...`} at analysis ({@code
   * TABLE_OR_VIEW_NOT_FOUND}) because {@code INSERT} DML requires a registered {@code
   * table_identifier} (see Spark 4.1 INSERT TABLE docs). Parsing still builds an {@code
   * InsertIntoStatement} whose target is an {@code UnresolvedRelation}, so {@link
   * ResolvePathCredentials} must inject credentials before analysis — this test pins that behavior
   * without executing the statement.
   */
  @Test
  public void testInsertIntoBarePathInjectsCredentialsAtParseTime()
      throws IOException, ParseException {
    session = createUcSparkSession(false, false, true, SPARK_CATALOG);
    String location = bucketPath("insert_into_parse");
    String insertSql = String.format("INSERT INTO parquet.`%s` SELECT 2 AS i, 'b' AS s", location);

    LogicalPlan plan = session.sessionState().sqlParser().parsePlan(insertSql);
    assertThat(plan).isInstanceOf(InsertIntoStatement.class);
    LogicalPlan table = ((InsertIntoStatement) plan).table();
    assertThat(table).isInstanceOf(UnresolvedRelation.class);
    UnresolvedRelation target = (UnresolvedRelation) table;
    assertThat(target.options().get("fs.s3a.access.key")).isEqualTo("accessKey0");

    assertThrows(Exception.class, () -> session.sql(insertSql).collectAsList());
  }

  /**
   * When the feature is disabled, no credentials are injected for the bare path, so the read fails
   * (this is the pre-fix behavior). Confirms the rule is what enables direct path access.
   */
  @Test
  public void testDisabledByFlag() throws IOException {
    session = createUcSparkSession(false, false, true, SPARK_CATALOG);
    String location = bucketPath("import_disabled");
    // Write with the feature on so the data exists.
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);

    stopSession();
    session = createUcSparkSession(false, false, false, SPARK_CATALOG);
    Exception thrown =
        assertThrows(Exception.class, () -> sql("SELECT * FROM parquet.`%s`", location));
    Throwable credentialError = thrown;
    while (credentialError != null
        && (credentialError.getMessage() == null
            || !credentialError.getMessage().contains("accessKey0"))) {
      credentialError = credentialError.getCause();
    }
    assertThat(credentialError).isNotNull();
    assertThat(credentialError.getMessage()).contains("accessKey0");
  }
}
