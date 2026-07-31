package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

  private static final String MERGE_TARGET_TABLE = "path_cred_merge_target";

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

  private static List<Throwable> causalChain(Throwable t) {
    List<Throwable> chain = new ArrayList<>();
    while (t != null) {
      chain.add(t);
      t = t.getCause();
    }
    return chain;
  }

  private static void assertNoCauseOfType(Throwable thrown, Class<?> type) {
    assertThat(causalChain(thrown)).noneMatch(type::isInstance);
  }

  private static void assertCauseChainContainsMessage(Throwable thrown, String substring) {
    assertThat(causalChain(thrown))
        .extracting(Throwable::getMessage)
        .anyMatch(msg -> msg != null && msg.contains(substring));
  }

  /** Finds a bare {@code format.`cloud-path`} relation anywhere in a parsed plan tree. */
  private static UnresolvedRelation findBareCloudPathRelation(LogicalPlan plan) {
    if (plan instanceof UnresolvedRelation) {
      UnresolvedRelation relation = (UnresolvedRelation) plan;
      if (relation.multipartIdentifier().length() == 2) {
        return relation;
      }
    }
    scala.collection.Seq<LogicalPlan> children = plan.children();
    for (int i = 0; i < children.length(); i++) {
      UnresolvedRelation found = findBareCloudPathRelation(children.apply(i));
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Writes to a bare cloud path and reads it back. Exercises {@code INSERT OVERWRITE DIRECTORY}
   * ({@code InsertIntoDir}) and {@code parquet.`path`} reads ({@code UnresolvedRelation}).
   * Bare-path {@code INSERT INTO} write targets are covered at parse time in {@link
   * #testInsertIntoBarePathInjectsCredentialsAtParseTime()} because Spark 4.x rejects them at
   * analysis ({@code TABLE_OR_VIEW_NOT_FOUND}) on all supported versions (4.0–4.2).
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

  /** UC extension only — enough for parameterized SQL parser tests (no Delta needed). */
  private SparkSession createUcExtensionOnlySparkSession(String... catalogs) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.extensions", "io.unitycatalog.spark.UCSparkSessionExtensions");
    for (String catalog : catalogs) {
      String catalogConf = "spark.sql.catalog." + catalog;
      builder =
          builder
              .config(catalogConf, UCSingleCatalog.class.getName())
              .config(catalogConf + "." + OptionsUtil.URI, serverConfig.getServerUrl())
              .config(catalogConf + "." + OptionsUtil.TOKEN, serverConfig.getAuthToken())
              .config(catalogConf + "." + OptionsUtil.WAREHOUSE, catalog);
    }
    return builder.getOrCreate();
  }

  /**
   * {@code SparkSession.sql(text, args)} routes through {@code parsePlanWithParameters}. The UC
   * parser extension must delegate parameter binding to the underlying parser before applying path
   * credential injection. Uses a UC-only session (no Delta extension) so the delegate is {@code
   * SparkSqlParser} directly.
   */
  @Test
  public void testPositionalSqlParameters() {
    stopSession();
    session = createUcExtensionOnlySparkSession(SPARK_CATALOG);
    List<Row> rows = session.sql("SELECT ? AS c", new Object[] {42}).collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(42);
  }

  @Test
  public void testNamedSqlParameters() {
    stopSession();
    session = createUcExtensionOnlySparkSession(SPARK_CATALOG);
    List<Row> rows = session.sql("SELECT :x AS c", Map.of("x", 42)).collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(42);
  }

  /**
   * Spark 4.x rejects {@code INSERT INTO parquet.`s3://...`} at analysis ({@code
   * TABLE_OR_VIEW_NOT_FOUND}) because {@code INSERT} DML requires a registered {@code
   * table_identifier}. Parsing still builds an {@code InsertIntoStatement} whose target is an
   * {@code UnresolvedRelation}, so {@link ResolvePathCredentials} must inject credentials before
   * analysis — this test pins parse-time credential injection only; it does not execute the DML.
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
  }

  /**
   * After {@code SET CATALOG}, bare-path credential injection must follow the session's current
   * catalog, not {@code SQLConf.DEFAULT_CATALOG}. Default remains the built-in {@code
   * spark_catalog} while the UC catalog is selected explicitly.
   */
  @Test
  public void testBarePathUsesCurrentCatalogAfterSetCatalog() throws IOException, ParseException {
    stopSession();
    session = createUcSparkSession(false, false, true, CATALOG_NAME);
    String location = bucketPath("use_catalog");
    sql("SET CATALOG %s", CATALOG_NAME);
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);

    LogicalPlan plan =
        session
            .sessionState()
            .sqlParser()
            .parsePlan(String.format("SELECT * FROM parquet.`%s`", location));
    UnresolvedRelation relation = findBareCloudPathRelation(plan);
    assertThat(relation)
        .as("parsed plan should contain bare cloud-path UnresolvedRelation: %s", plan)
        .isNotNull();
    assertThat(relation.options().get("fs.s3a.access.key")).isEqualTo("accessKey0");
    assertSingleRow(sql("SELECT * FROM parquet.`%s`", location));
  }

  /**
   * Path credential vending must use the explicit {@code SparkSession} passed from the parser, not
   * {@code SparkSession.getActiveSession}. {@code
   * session.sessionState().sqlParser().parsePlan(...)} does not guarantee an active session on the
   * calling thread.
   */
  @Test
  public void testVendPathCredentialsWithoutActiveSession() throws IOException {
    session = createUcSparkSession(false, false, true, SPARK_CATALOG);
    String location = bucketPath("no_active_session");
    UCSingleCatalog catalog =
        (UCSingleCatalog) session.sessionState().catalogManager().catalog(SPARK_CATALOG);

    SparkSession.clearActiveSession();
    try {
      assertThat(catalog.vendPathCredentialConfWithFallback(session, location))
          .containsEntry("fs.s3a.access.key", "accessKey0");
    } finally {
      SparkSession.setActiveSession(session);
    }
  }

  /**
   * When UC cannot vend credentials for a path (not managed by UC), execution continues and Spark
   * falls back to ambient storage credentials. With none configured for this bucket, the write
   * fails at the filesystem layer — not with a UC {@link ApiException}.
   */
  @Test
  public void testFallsBackToAmbientWhenPathNotManagedByUc() throws IOException {
    session = createUcSparkSession(false, false, true, SPARK_CATALOG);
    String location = "s3://" + NO_CREDS_BUCKET + new File(dataDir, "unmanaged").getCanonicalPath();

    assertThatThrownBy(
            () ->
                sql(
                    "INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s",
                    location))
        .satisfies(e -> assertNoCauseOfType(e, ApiException.class))
        .satisfies(e -> assertCauseChainContainsMessage(e, "invalid path"));
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
    assertThatThrownBy(() -> sql("SELECT * FROM parquet.`%s`", location))
        .satisfies(e -> assertCauseChainContainsMessage(e, "accessKey0"));
  }

  /**
   * Storium import row-count SQL wraps bare paths in a subquery: {@code SELECT COUNT(*) FROM
   * (SELECT * FROM parquet.`s3://...`)}.
   */
  @Test
  public void testCountFromParquetSubquery() throws IOException {
    stopSession();
    session = createUcSparkSession(false, false, true, SPARK_CATALOG, CATALOG_NAME);
    String location = bucketPath("count_subquery");
    sql("SET CATALOG %s", CATALOG_NAME);
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);

    List<Row> rows = sql("SELECT COUNT(*) AS c FROM (SELECT * FROM parquet.`%s`)", location);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getLong(0)).isEqualTo(1);
  }

  private String mergeTargetTable() {
    return String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, MERGE_TARGET_TABLE);
  }

  /**
   * Storium import MERGE shape: {@code USING (SELECT cols FROM (SELECT * FROM parquet.`path`))}.
   * With {@code SET CATALOG} pointing at the UC catalog (as Thrift sessions do when the JDBC URL
   * includes a catalog), bare paths must stay 2-part relations after parse — not {@code
   * catalog.parquet.s3://...}.
   */
  @Test
  public void testMergeIntoUsingParquetSubquery() throws IOException, ParseException {
    stopSession();
    session = createUcSparkSession(false, false, true, SPARK_CATALOG, CATALOG_NAME);
    String location = bucketPath("merge_subquery");
    sql("SET CATALOG %s", CATALOG_NAME);
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);

    String target = mergeTargetTable();
    sql("CREATE TABLE %s (i INT, s STRING) USING delta", target);

    String mergeSql =
        String.format(
            "MERGE INTO %s trgt "
                + "USING (SELECT i, s FROM (SELECT * FROM parquet.`%s`)) src "
                + "ON false "
                + "WHEN NOT MATCHED THEN INSERT *",
            target, location);

    LogicalPlan plan = session.sessionState().sqlParser().parsePlan(mergeSql);
    UnresolvedRelation relation = findBareCloudPathRelation(plan);
    assertThat(relation)
        .as("parsed plan should contain bare cloud-path UnresolvedRelation: %s", plan)
        .isNotNull();
    assertThat(relation.options().get("fs.s3a.access.key")).isEqualTo("accessKey0");

    session.sql(mergeSql).collect();
    assertSingleRow(sql("SELECT * FROM %s", target));
  }

  /**
   * Same MERGE shape as {@link #testMergeIntoUsingParquetSubquery()} but relies on {@code
   * spark.sql.defaultCatalog} instead of {@code SET CATALOG} — closer to schema-only JDBC URLs
   * where the session catalog is configured up front.
   */
  @Test
  public void testMergeIntoParquetSubqueryWithDefaultCatalog() throws IOException {
    stopSession();
    session = createUcSparkSession(false, false, true, SPARK_CATALOG, CATALOG_NAME);
    session.conf().set("spark.sql.defaultCatalog", CATALOG_NAME);
    String location = bucketPath("merge_default_catalog");
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 2 AS i, 'b' AS s", location);

    String target = mergeTargetTable() + "_default";
    sql("CREATE TABLE %s (i INT, s STRING) USING delta", target);

    session
        .sql(
            String.format(
                "MERGE INTO %s trgt "
                    + "USING (SELECT i, s FROM (SELECT * FROM parquet.`%s`)) src "
                    + "ON false "
                    + "WHEN NOT MATCHED THEN INSERT *",
                target, location))
        .collect();

    assertThat(sql("SELECT * FROM %s", target)).hasSize(1);
    assertThat(sql("SELECT * FROM %s", target).get(0).getInt(0)).isEqualTo(2);
    assertThat(sql("SELECT * FROM %s", target).get(0).getString(1)).isEqualTo("b");
  }

  /**
   * Thrift JDBC pushes {@code hiveconf:spark.sql.defaultCatalog} at session creation — not via
   * runtime {@code conf().set} or {@code SET CATALOG}. {@link ResolvePathCredentials} keys off
   * {@code catalogManager.currentCatalog}, which may still be {@code spark_catalog} even when
   * defaultCatalog points at the UC catalog.
   */
  @Test
  public void testCountFromParquetSubqueryDefaultCatalogAtSessionStart() throws IOException {
    stopSession();
    session =
        createUcSparkSessionWithDefaultCatalog(
            CATALOG_NAME, false, false, true, SPARK_CATALOG, CATALOG_NAME);
    String location = bucketPath("count_session_start_default");
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);

    List<Row> rows = sql("SELECT COUNT(*) AS c FROM (SELECT * FROM parquet.`%s`)", location);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getLong(0)).isEqualTo(1);
  }

  /**
   * Storium Thrift layout: {@code spark_catalog=DeltaCatalog}, tenant {@code UCSingleCatalog},
   * {@code defaultCatalog=tenant}, {@code current_catalog=spark_catalog}. Path cred is skipped;
   * Thrift e2e fails with {@code Invalid table name: tenant.parquet.s3://…}; fake FS may instead
   * report missing vended credentials.
   */
  @Test
  public void testCountFromParquetSubqueryFailsWhenCurrentCatalogIsDeltaCatalog()
      throws IOException {
    stopSession();
    session = createStoriumLikeThriftSession(CATALOG_NAME, false);
    session.conf().set("spark.sql.defaultCatalog", CATALOG_NAME);
    sql("SET CATALOG %s", CATALOG_NAME);
    String location = bucketPath("storium_like_thrift");
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);
    sql("SET CATALOG %s", SPARK_CATALOG);

    assertThat(sql("SELECT current_catalog()").get(0).getString(0)).isEqualTo(SPARK_CATALOG);
    assertThat(session.conf().get("spark.sql.defaultCatalog")).isEqualTo(CATALOG_NAME);

    assertThatThrownBy(
            () -> sql("SELECT COUNT(*) AS c FROM (SELECT * FROM parquet.`%s`)", location))
        .isNotNull();
  }

  /** {@code SET CATALOG} to the tenant UC catalog is the Storium-side workaround. */
  @Test
  public void testCountFromParquetSubqueryWorksAfterSetCatalogToTenant() throws IOException {
    stopSession();
    session = createStoriumLikeThriftSession(CATALOG_NAME, false);
    session.conf().set("spark.sql.defaultCatalog", CATALOG_NAME);
    sql("SET CATALOG %s", CATALOG_NAME);
    String location = bucketPath("storium_set_catalog");
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);

    List<Row> rows = sql("SELECT COUNT(*) AS c FROM (SELECT * FROM parquet.`%s`)", location);
    assertThat(rows.get(0).getLong(0)).isEqualTo(1);
  }

  /**
   * {@code conf().set("spark.sql.defaultCatalog", …)} switches {@code current_catalog} to the
   * tenant UC catalog — unlike Thrift {@code hiveconf}, which can leave {@code current_catalog} on
   * Delta.
   */
  @Test
  public void testCountFromParquetSubqueryWorksAfterRuntimeDefaultCatalog() throws IOException {
    stopSession();
    session = createStoriumLikeThriftSession(CATALOG_NAME, false);
    session.conf().set("spark.sql.defaultCatalog", CATALOG_NAME);

    String location = bucketPath("storium_runtime_default");
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);

    assertThat(sql("SELECT current_catalog()").get(0).getString(0)).isEqualTo(CATALOG_NAME);
    List<Row> rows = sql("SELECT COUNT(*) AS c FROM (SELECT * FROM parquet.`%s`)", location);
    assertThat(rows.get(0).getLong(0)).isEqualTo(1);
  }

  /** Mirrors celospark spark-uc.properties + Storium per-session hiveconf catalog layout. */
  private SparkSession createStoriumLikeThriftSession(String tenantCatalog) {
    return createStoriumLikeThriftSession(tenantCatalog, true);
  }

  private SparkSession createStoriumLikeThriftSession(
      String tenantCatalog, boolean defaultCatalogAtStartup) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension,"
                    + "io.unitycatalog.spark.UCSparkSessionExtensions")
            // Driver static (spark-uc.properties)
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            // Per-session hiveconf (SparkUnityCatalogDatasourceMapper)
            .config("spark.sql.catalog." + tenantCatalog, UCSingleCatalog.class.getName())
            .config(
                "spark.sql.catalog." + tenantCatalog + "." + OptionsUtil.URI,
                serverConfig.getServerUrl())
            .config(
                "spark.sql.catalog." + tenantCatalog + "." + OptionsUtil.TOKEN,
                serverConfig.getAuthToken())
            .config(
                "spark.sql.catalog." + tenantCatalog + "." + OptionsUtil.WAREHOUSE, tenantCatalog)
            .config(
                "spark.sql.catalog."
                    + tenantCatalog
                    + "."
                    + OptionsUtil.VEND_PATH_CREDENTIALS_ENABLED,
                true);
    if (defaultCatalogAtStartup) {
      builder.config("spark.sql.defaultCatalog", tenantCatalog);
    }
    builder.config("spark.hadoop.fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    return builder.getOrCreate();
  }

  private SparkSession createUcSparkSessionWithDefaultCatalog(
      String defaultCatalog,
      boolean renewCred,
      boolean credScopedFsEnabled,
      boolean vendPathCredentialsEnabled,
      String... catalogs) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.defaultCatalog", defaultCatalog)
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
    builder.config("spark.hadoop.fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    return builder.getOrCreate();
  }
}
