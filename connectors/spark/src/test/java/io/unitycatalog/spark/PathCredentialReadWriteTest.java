package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for {@link ResolvePathCredentials}: Unity Catalog credentials are vended for cloud paths
 * referenced directly in a query (e.g. {@code parquet.`s3://bucket/dir`}), without a pre-registered
 * external table. Bare {@code delta.`s3://...`} paths are excluded and continue to use ambient
 * storage credentials until Delta execution support lands separately.
 *
 * <p>Unlike the other Spark integration tests, these register {@code UCSparkSessionExtensions} (the
 * home of the analyzer hint resolution rule), and use the {@link S3CredentialTestFileSystem} fake
 * filesystem to assert the vended credentials reach S3A. The test principal is the metastore owner,
 * so path authorization passes and credentials fall back to the per-bucket server config ({@code
 * accessKey0}/... for {@code s3://test-bucket0}) configured in {@link
 * BaseSparkIntegrationTest#setUpProperties()}.
 */
public class PathCredentialReadWriteTest extends BaseSparkIntegrationTest {

  private static final String MERGE_TARGET_TABLE = "path_cred_merge_target";
  private static final String DELTA_AND_UC_EXTENSIONS =
      "io.delta.sql.DeltaSparkSessionExtension," + "io.unitycatalog.spark.UCSparkSessionExtensions";
  private static final String DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog";

  @TempDir protected File dataDir;

  private SparkSession.Builder configureUcCatalog(SparkSession.Builder builder, String catalog) {
    String catalogConf = "spark.sql.catalog." + catalog;
    return builder
        .config(catalogConf, UCSingleCatalog.class.getName())
        .config(catalogConf + "." + OptionsUtil.URI, serverConfig.getServerUrl())
        .config(catalogConf + "." + OptionsUtil.TOKEN, serverConfig.getAuthToken())
        .config(catalogConf + "." + OptionsUtil.WAREHOUSE, catalog);
  }

  private SparkSession.Builder configureUcCatalogWithPathCredOptions(
      SparkSession.Builder builder,
      String catalog,
      boolean renewCred,
      boolean credScopedFs,
      boolean vendPathEnabled) {
    String catalogConf = "spark.sql.catalog." + catalog;
    return configureUcCatalog(builder, catalog)
        .config(catalogConf + "." + OptionsUtil.RENEW_CREDENTIAL_ENABLED, renewCred)
        .config(catalogConf + "." + OptionsUtil.CRED_SCOPED_FS_ENABLED, credScopedFs)
        .config(catalogConf + "." + OptionsUtil.VEND_PATH_CREDENTIALS_ENABLED, vendPathEnabled);
  }

  /** Default path-credential session: Delta + UC extensions, fake S3 FS, path cred enabled. */
  private SparkSession createPathCredSession(String... ucCatalogs) {
    return createPathCredSession(false, false, true, ucCatalogs);
  }

  private SparkSession createPathCredSession(
      boolean renewCred, boolean credScopedFs, boolean vendPathEnabled, String... ucCatalogs) {
    return createPathCredSessionWithLayout(
        renewCred, credScopedFs, vendPathEnabled, null, null, ucCatalogs);
  }

  /** UC catalog selected via {@code spark.sql.defaultCatalog} at session creation. */
  private SparkSession createPathCredSessionWithDefaultCatalogAtStartup(
      String defaultCatalog, String... ucCatalogs) {
    return createPathCredSessionWithLayout(false, false, true, defaultCatalog, null, ucCatalogs);
  }

  private SparkSession createPathCredSessionWithLayout(
      boolean renewCred,
      boolean credScopedFs,
      boolean vendPathEnabled,
      String defaultCatalogAtStartup,
      String sparkCatalogImpl,
      String... ucCatalogs) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.extensions", DELTA_AND_UC_EXTENSIONS);
    if (sparkCatalogImpl != null) {
      builder.config("spark.sql.catalog.spark_catalog", sparkCatalogImpl);
    }
    if (defaultCatalogAtStartup != null) {
      builder.config("spark.sql.defaultCatalog", defaultCatalogAtStartup);
    }
    for (String catalog : ucCatalogs) {
      builder =
          configureUcCatalogWithPathCredOptions(
              builder, catalog, renewCred, credScopedFs, vendPathEnabled);
    }
    builder
        .config("spark.hadoop.fs.s3.impl", S3CredentialTestFileSystem.class.getName())
        .config("spark.hadoop.fs.s3a.impl", S3CredentialTestFileSystem.S3a.class.getName());
    return builder.getOrCreate();
  }

  /** UC extension only — parameterized SQL tests delegate to {@code SparkSqlParser} directly. */
  private SparkSession createUcExtensionOnlySession(String... catalogs) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.extensions", "io.unitycatalog.spark.UCSparkSessionExtensions");
    for (String catalog : catalogs) {
      builder = configureUcCatalog(builder, catalog);
    }
    return builder.getOrCreate();
  }

  /**
   * Session with {@code spark_catalog} wired to Delta and a separate {@link UCSingleCatalog}
   * registered under {@code ucCatalog}.
   */
  private SparkSession createDeltaSparkCatalogSession(
      String ucCatalog, boolean defaultCatalogAtStartup) {
    return createPathCredSessionWithLayout(
        false, false, true, defaultCatalogAtStartup ? ucCatalog : null, DELTA_CATALOG, ucCatalog);
  }

  /**
   * Ways a session can select the UC catalog as {@code current_catalog} before a bare-path count
   * subquery succeeds. Each constant owns session creation and any post-open catalog setup.
   */
  private enum CountSubqueryLayout {
    SET_CATALOG_AFTER_STARTUP("count_subquery") {
      @Override
      SparkSession openSession(PathCredentialReadWriteTest test) {
        return test.createPathCredSession(SPARK_CATALOG, CATALOG_NAME);
      }

      @Override
      void prepareSession(PathCredentialReadWriteTest test) {
        test.sql("SET CATALOG %s", CATALOG_NAME);
      }
    },
    DEFAULT_CATALOG_AT_SESSION_START("count_session_start_default") {
      @Override
      SparkSession openSession(PathCredentialReadWriteTest test) {
        return test.createPathCredSessionWithDefaultCatalogAtStartup(
            CATALOG_NAME, SPARK_CATALOG, CATALOG_NAME);
      }
    },
    SET_CATALOG_WITH_DELTA_SPARK_CATALOG("delta_spark_catalog_set_uc") {
      @Override
      SparkSession openSession(PathCredentialReadWriteTest test) {
        return test.createDeltaSparkCatalogSession(CATALOG_NAME, false);
      }

      @Override
      void prepareSession(PathCredentialReadWriteTest test) {
        test.session.conf().set("spark.sql.defaultCatalog", CATALOG_NAME);
        test.sql("SET CATALOG %s", CATALOG_NAME);
      }
    },
    RUNTIME_DEFAULT_CATALOG_WITH_DELTA_SPARK_CATALOG("delta_spark_catalog_runtime_default") {
      @Override
      SparkSession openSession(PathCredentialReadWriteTest test) {
        return test.createDeltaSparkCatalogSession(CATALOG_NAME, false);
      }

      @Override
      void prepareSession(PathCredentialReadWriteTest test) {
        test.session.conf().set("spark.sql.defaultCatalog", CATALOG_NAME);
      }

      @Override
      void assertPreconditions(PathCredentialReadWriteTest test) {
        assertThat(test.sql("SELECT current_catalog()").get(0).getString(0))
            .isEqualTo(CATALOG_NAME);
      }
    };

    final String pathSuffix;

    CountSubqueryLayout(String pathSuffix) {
      this.pathSuffix = pathSuffix;
    }

    abstract SparkSession openSession(PathCredentialReadWriteTest test);

    void prepareSession(PathCredentialReadWriteTest test) {}

    void assertPreconditions(PathCredentialReadWriteTest test) {}
  }

  private void writeBareParquetSample(String location) {
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);
  }

  private void assertCountSubquerySucceeds(String location) {
    List<Row> rows = sql("SELECT COUNT(*) AS c FROM (SELECT * FROM parquet.`%s`)", location);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getLong(0)).isEqualTo(1);
  }

  /**
   * A bare {@code s3://test-bucket0/...} path backed by a local temp dir understood by the fake FS.
   */
  private String bucketPath(String name) throws IOException {
    return bucketPath("s3", name);
  }

  private String bucketPath(String scheme, String name) throws IOException {
    return scheme + "://test-bucket0" + new File(dataDir, name).getCanonicalPath();
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

  /** Applies the analyzer path-credential rule to a parsed plan (parser is side-effect free). */
  private LogicalPlan injectPathCredentials(LogicalPlan plan) {
    return new ResolvePathCredentials(session).apply(plan);
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
   * Bare-path {@code INSERT INTO} write targets are covered by {@link
   * #testInsertIntoBarePathInjectsCredentials()} because Spark 4.x rejects them at analysis ({@code
   * TABLE_OR_VIEW_NOT_FOUND}) on all supported versions (4.0–4.2).
   */
  @ParameterizedTest
  @CsvSource({"s3,  false, false", "s3,  true,  true", "s3a, false, false"})
  public void testWriteAndReadBarePath(
      String scheme, boolean renewCred, boolean credScopedFsEnabled)
      throws IOException, ParseException {
    session = createPathCredSession(renewCred, credScopedFsEnabled, true, SPARK_CATALOG);
    String location =
        bucketPath(
            scheme, "write_directory_" + scheme + "_" + renewCred + "_" + credScopedFsEnabled);

    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);

    if ("s3a".equalsIgnoreCase(scheme)) {
      LogicalPlan readPlan =
          injectPathCredentials(
              session
                  .sessionState()
                  .sqlParser()
                  .parsePlan(String.format("SELECT * FROM parquet.`%s`", location)));
      UnresolvedRelation relation = findBareCloudPathRelation(readPlan);
      assertThat(relation).isNotNull();
      assertThat(relation.options().get("fs.s3a.access.key")).isEqualTo("accessKey0");
      assertThat(relation.options().get(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY))
          .isEqualTo(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE);
      assertThat(relation.options().get(UCHadoopConfConstants.UC_PATH_KEY))
          .isEqualTo(location.replaceFirst("(?i)^s3a://", "s3://"));
    }

    assertSingleRow(sql("SELECT * FROM parquet.`%s`", location));
  }

  /**
   * Exercises bare {@code delta.`...`} path tables with {@code DeltaCatalog} on {@code
   * spark_catalog} and the Delta session extension.
   *
   * <p>Local storage: end-to-end CREATE + read (mirrors {@link
   * DeltaExternalTableReadWriteTest#testDeltaPathTable()} under the path-cred session layout).
   *
   * <p>Cloud storage: seeds data via a UC catalog table, then asserts {@link
   * ResolvePathCredentials} does not inject credentials into {@code delta.`s3://...`} (Delta
   * bare-path execution with UC-vended credentials is tracked in a follow-up PR).
   */
  @Test
  public void testWriteAndReadBareDeltaPath() throws IOException, ParseException {
    session = createDeltaSparkCatalogSession(CATALOG_NAME, false);
    sql("SET CATALOG %s", CATALOG_NAME);

    String localPath = new File(dataDir, "delta_local_path").getCanonicalPath();
    sql("CREATE TABLE delta.`%s` USING delta AS SELECT 1 AS i, 'a' AS s", localPath);
    assertSingleRow(sql("SELECT * FROM delta.`%s`", localPath));

    String s3Location = bucketPath("delta_path");
    String seedTable = String.format("%s.%s.path_cred_delta_seed", CATALOG_NAME, SCHEMA_NAME);
    sql("CREATE TABLE %s (i INT, s STRING) USING delta LOCATION '%s'", seedTable, s3Location);
    sql("INSERT INTO %s SELECT 1 AS i, 'a' AS s", seedTable);

    LogicalPlan readPlan =
        injectPathCredentials(
            session
                .sessionState()
                .sqlParser()
                .parsePlan(String.format("SELECT * FROM delta.`%s`", s3Location)));
    UnresolvedRelation relation = findBareCloudPathRelation(readPlan);
    assertThat(relation).isNotNull();
    assertThat(relation.options().get("fs.s3a.access.key")).isNull();
  }

  /**
   * {@code SparkSession.sql(text, args)} must still bind parameters when the UC analyzer extension
   * is registered. Uses a UC-only session (no Delta extension).
   */
  @Test
  public void testPositionalSqlParameters() {
    session = createUcExtensionOnlySession(SPARK_CATALOG);
    List<Row> rows = session.sql("SELECT ? AS c", new Object[] {42}).collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(42);
  }

  @Test
  public void testNamedSqlParameters() {
    session = createUcExtensionOnlySession(SPARK_CATALOG);
    List<Row> rows = session.sql("SELECT :x AS c", Map.of("x", 42)).collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(42);
  }

  /**
   * Spark 4.x rejects {@code INSERT INTO parquet.`s3://...`} at analysis ({@code
   * TABLE_OR_VIEW_NOT_FOUND}) because {@code INSERT} DML requires a registered {@code
   * table_identifier}. The optional {@code InsertIntoStatement} branch still injects credentials
   * onto the target {@code UnresolvedRelation} (not a tree child). This test applies the analyzer
   * rule to the parsed plan; it does not execute the DML.
   */
  @Test
  public void testInsertIntoBarePathInjectsCredentials() throws IOException, ParseException {
    session = createPathCredSession(SPARK_CATALOG);
    String location = bucketPath("insert_into_bare_path");
    String insertSql = String.format("INSERT INTO parquet.`%s` SELECT 2 AS i, 'b' AS s", location);

    LogicalPlan plan =
        injectPathCredentials(session.sessionState().sqlParser().parsePlan(insertSql));
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
    session = createPathCredSession(CATALOG_NAME);
    String location = bucketPath("use_catalog");
    sql("SET CATALOG %s", CATALOG_NAME);
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);

    LogicalPlan plan =
        injectPathCredentials(
            session
                .sessionState()
                .sqlParser()
                .parsePlan(String.format("SELECT * FROM parquet.`%s`", location)));
    UnresolvedRelation relation = findBareCloudPathRelation(plan);
    assertThat(relation)
        .as(
            "analyzer rule should inject credentials onto bare cloud-path UnresolvedRelation: %s",
            plan)
        .isNotNull();
    assertThat(relation.options().get("fs.s3a.access.key")).isEqualTo("accessKey0");
    assertSingleRow(sql("SELECT * FROM parquet.`%s`", location));
  }

  /**
   * The analyzer runs its batches to a fixed point, so the rule is applied to the same plan more
   * than once. Credentials must be vended only on the first pass: re-vending would issue a UC
   * request per iteration and, with the credential cache off, hand back a fresh session token every
   * time, so the plan would never stabilize and the batch would hit its iteration limit.
   */
  @Test
  public void testRepeatedApplicationVendsOnceAndLeavesPlanUnchanged()
      throws IOException, ParseException {
    session = createPathCredSession(SPARK_CATALOG);
    session.conf().set(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, "false");
    String location = bucketPath("repeated_rule_application");

    AtomicInteger vends = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            () ->
                List.of(
                    new AwsCredential(
                        "accessKey0",
                        "secretKey0",
                        "sessionToken" + vends.incrementAndGet(),
                        Long.MAX_VALUE,
                        null));
    try {
      LogicalPlan parsed =
          session
              .sessionState()
              .sqlParser()
              .parsePlan(String.format("SELECT * FROM parquet.`%s`", location));
      LogicalPlan first = injectPathCredentials(parsed);
      LogicalPlan second = injectPathCredentials(first);

      assertThat(vends.get()).isEqualTo(1);
      assertThat(second.fastEquals(first)).as("rule must reach a fixed point: %s", second).isTrue();
      assertThat(findBareCloudPathRelation(first).options().get("fs.s3a.session.token"))
          .isEqualTo("sessionToken1");
    } finally {
      CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    }
  }

  /**
   * {@code s3://} and {@code s3a://} both vend S3 credentials as {@code fs.s3a.*} keys; {@code
   * s3a://} is rewritten to {@code s3://} for UC path-credential lookup only. Scheme casing is
   * normalized to lowercase for UC API calls ({@code S3://}, {@code S3A://}, etc.).
   */
  @ParameterizedTest
  @CsvSource({"s3", "s3a", "S3", "S3A"})
  public void testVendPathCredentialsForScheme(String scheme) throws IOException {
    session = createPathCredSession(SPARK_CATALOG);
    String location = bucketPath(scheme, "vend_" + scheme);
    UCSingleCatalog catalog =
        (UCSingleCatalog) session.sessionState().catalogManager().catalog(SPARK_CATALOG);

    String identityPath = location.replaceFirst("(?i)^(s3a|s3)://", "s3://");
    assertThat(catalog.vendPathCredentialConfWithFallback(session, location))
        .containsEntry("fs.s3a.access.key", "accessKey0")
        .containsEntry(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .containsEntry(UCHadoopConfConstants.UC_PATH_KEY, identityPath);
  }

  /**
   * Path credential vending must use the explicit {@code SparkSession} passed into the analyzer
   * rule, not {@code SparkSession.getActiveSession}.
   */
  @Test
  public void testVendPathCredentialsWithoutActiveSession() throws IOException {
    session = createPathCredSession(SPARK_CATALOG);
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
    session = createPathCredSession(SPARK_CATALOG);
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
   * An allowed UC miss (unmanaged path) still stamps path-cred identity so a later analyzer pass
   * does not re-issue the failing RPCs, including when the credential cache is disabled.
   */
  @Test
  public void testAllowedMissStampsIdentityAndSecondApplyLeavesPlanUnchanged()
      throws IOException, ParseException {
    session = createPathCredSession(SPARK_CATALOG);
    session.conf().set(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, "false");
    String location =
        "s3://" + NO_CREDS_BUCKET + new File(dataDir, "miss_identity").getCanonicalPath();

    LogicalPlan parsed =
        session
            .sessionState()
            .sqlParser()
            .parsePlan(String.format("SELECT * FROM parquet.`%s`", location));
    LogicalPlan firstPlan = injectPathCredentials(parsed);
    UnresolvedRelation first = findBareCloudPathRelation(firstPlan);
    assertThat(first).isNotNull();
    assertThat(first.options().get("fs.s3a.access.key")).isNull();
    assertThat(first.options().get(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY))
        .isEqualTo(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE);
    assertThat(first.options().get(UCHadoopConfConstants.UC_PATH_KEY)).isEqualTo(location);

    LogicalPlan second = injectPathCredentials(firstPlan);
    UnresolvedRelation after = findBareCloudPathRelation(second);
    assertThat(after.options().asCaseSensitiveMap())
        .containsExactlyInAnyOrderEntriesOf(first.options().asCaseSensitiveMap());
  }

  /**
   * When the feature is disabled, no credentials are injected for the bare path, so the read fails
   * (this is the pre-fix behavior). Confirms the rule is what enables direct path access.
   */
  @Test
  public void testDisabledByFlag() throws IOException {
    session = createPathCredSession(SPARK_CATALOG);
    String location = bucketPath("import_disabled");
    // Write with the feature on so the data exists.
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", location);

    // Recreate session with path credential vending disabled.
    stopSession();
    session = createPathCredSession(false, false, false, SPARK_CATALOG);
    assertThatThrownBy(() -> sql("SELECT * FROM parquet.`%s`", location))
        .satisfies(e -> assertCauseChainContainsMessage(e, "accessKey0"));
  }

  /**
   * Row-count SQL that wraps a bare path in a subquery: {@code SELECT COUNT(*) FROM (SELECT * FROM
   * parquet.`s3://...`)}. Each layout exercises a different way of selecting the UC catalog as
   * {@code current_catalog} before the count runs.
   */
  @ParameterizedTest(name = "{0}")
  @EnumSource(CountSubqueryLayout.class)
  public void testCountFromParquetSubquery(CountSubqueryLayout layout) throws IOException {
    session = layout.openSession(this);
    layout.prepareSession(this);
    String location = bucketPath(layout.pathSuffix);
    writeBareParquetSample(location);
    layout.assertPreconditions(this);
    assertCountSubquerySucceeds(location);
  }

  private String mergeTargetTable() {
    return String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, MERGE_TARGET_TABLE);
  }

  /**
   * MERGE with a nested bare-path subquery: {@code USING (SELECT cols FROM (SELECT * FROM
   * parquet.`path`))}. With {@code SET CATALOG} pointing at the UC catalog, bare paths must stay
   * 2-part relations after parse — not {@code catalog.parquet.s3://...}.
   */
  @Test
  public void testMergeIntoUsingParquetSubquery() throws IOException, ParseException {
    session = createPathCredSession(SPARK_CATALOG, CATALOG_NAME);
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

    LogicalPlan plan =
        injectPathCredentials(session.sessionState().sqlParser().parsePlan(mergeSql));
    UnresolvedRelation relation = findBareCloudPathRelation(plan);
    assertThat(relation)
        .as(
            "analyzer rule should inject credentials onto bare cloud-path UnresolvedRelation: %s",
            plan)
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
    session = createPathCredSession(SPARK_CATALOG, CATALOG_NAME);
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
   * When {@code spark_catalog} is {@code DeltaCatalog} and {@code current_catalog} stays on it,
   * path credentials are not vended even if {@code spark.sql.defaultCatalog} points at a UC catalog
   * — the count subquery fails with missing vended credentials.
   */
  @Test
  public void testCountFromParquetSubqueryFailsWhenCurrentCatalogIsDeltaCatalog()
      throws IOException {
    session = createDeltaSparkCatalogSession(CATALOG_NAME, false);
    session.conf().set("spark.sql.defaultCatalog", CATALOG_NAME);
    sql("SET CATALOG %s", CATALOG_NAME);
    String location = bucketPath("delta_spark_catalog_current");
    writeBareParquetSample(location);
    sql("SET CATALOG %s", SPARK_CATALOG);

    assertThat(sql("SELECT current_catalog()").get(0).getString(0)).isEqualTo(SPARK_CATALOG);
    assertThat(session.conf().get("spark.sql.defaultCatalog")).isEqualTo(CATALOG_NAME);

    assertThatThrownBy(() -> assertCountSubquerySucceeds(location))
        .satisfies(e -> assertCauseChainContainsMessage(e, "accessKey0"));
  }
}
