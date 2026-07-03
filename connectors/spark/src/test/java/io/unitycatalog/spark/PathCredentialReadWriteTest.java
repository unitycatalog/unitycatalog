package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.spark.utils.OptionsUtil;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

  private static final String VEND_ENABLED_CONF =
      "spark.sql.unitycatalog.vendPathCredentials.enabled";

  @TempDir protected File dataDir;

  /**
   * Builds a Spark session that registers {@code UCSparkSessionExtensions} (so the parser-level
   * {@link ResolvePathCredentials} hook is active) and points the given catalogs at the test UC
   * server. Mirrors {@link BaseSparkIntegrationTest#createSparkSessionWithCatalogs} but adds the UC
   * extension. The catalogs are expected to already exist (created in {@code setUp}).
   */
  private SparkSession createUcSparkSession(
      boolean renewCred, boolean credScopedFsEnabled, String... catalogs) {
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
              .config(catalogConf + "." + OptionsUtil.CRED_SCOPED_FS_ENABLED, credScopedFsEnabled);
    }
    // Use fake file system for cloud storage so that we can assert vended credentials.
    builder.config("spark.hadoop.fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    return builder.getOrCreate();
  }

  /** A bare `s3://test-bucket0/...` path backed by a local temp dir understood by the fake FS. */
  private String bucketPath(String name) throws IOException {
    return "s3://test-bucket0" + new File(dataDir, name).getCanonicalPath();
  }

  private void assertSingleRow(List<Row> rows) {
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows.get(0).getString(1)).isEqualTo("a");
  }

  /**
   * Writes to a bare cloud path with `INSERT OVERWRITE DIRECTORY` (exercises the write branch) and
   * reads it back with `parquet.`<path>`` (exercises the read branch). Both previously failed
   * because path-based relations bypass the UC catalog and never got credentials.
   */
  @Test
  public void testWriteAndReadBareS3PathStaticCreds() throws IOException {
    session = createUcSparkSession(false, false, SPARK_CATALOG);
    String location = bucketPath("import_static");

    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);
    assertSingleRow(sql("SELECT * FROM parquet.`%s`", location));
  }

  /**
   * Same as above but with credential renewal + credential-scoped filesystem enabled, so the vended
   * credentials flow through {@code AwsVendedTokenProvider} + {@code CredScopedFileSystem} rather
   * than static keys.
   */
  @Test
  public void testWriteAndReadBareS3PathVendedProvider() throws IOException {
    session = createUcSparkSession(true, true, SPARK_CATALOG);
    String location = bucketPath("import_vended");

    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);
    assertSingleRow(sql("SELECT * FROM parquet.`%s`", location));
  }

  /**
   * When the feature is disabled, no credentials are injected for the bare path, so the read fails
   * (this is the pre-fix behavior). Confirms the rule is what enables direct path access.
   */
  @Test
  public void testDisabledByFlag() throws IOException {
    session = createUcSparkSession(false, false, SPARK_CATALOG);
    String location = bucketPath("import_disabled");
    // Write with the feature on so the data exists.
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);

    session.conf().set(VEND_ENABLED_CONF, "false");
    assertThatThrownBy(() -> sql("SELECT * FROM parquet.`%s`", location));
  }
}
