package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.migrate.SchemaMigrator.MigrationOutcome;
import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.flywaydb.core.api.MigrationInfo;
import org.junit.jupiter.api.Test;

public class MigratorTest {

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void setsUpAnEmptyDatabase() {
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    assertThat(migrator.state()).isEqualTo(SchemaState.EMPTY);

    MigrationOutcome outcome = migrator.migrate();

    assertThat(outcome.to()).isEqualTo(migrator.latestVersion());
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void setsUpADatabaseWithOnlyTheAuthorizationRulesTable() {
    execute(db, "CREATE TABLE casbin_rule (id INT)");
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    assertThat(migrator.state()).isEqualTo(SchemaState.EMPTY);

    assertThat(migrator.migrate().applied()).isEqualTo(releaseSteps(migrator));
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void adoptsA07Database() {
    create07Schema(db);
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    assertThat(migrator.state()).isEqualTo(SchemaState.UNVERSIONED_UC);

    MigrationOutcome outcome = migrator.migrate();

    String latest = migrator.latestVersion();
    assertThat(outcome).isEqualTo(new MigrationOutcome("1", latest, releaseSteps(migrator) - 1));
    MigrationInfo baseline = migrator.applied().get(0);
    assertThat(baseline.getType().isBaseline()).isTrue();
    assertThat(baseline.getVersion().getVersion()).isEqualTo("1");
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void refusesADatabaseFromBeforeTheBaselineRelease() {
    create07Schema(db);
    execute(db, "DROP TABLE uc_storage_cleanup_tasks");
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());

    assertThatThrownBy(new SchemaMigrator(db.dataSource(), db.kind())::migrate)
        .hasMessageContaining("before " + SchemaMigrator.BASELINE_RELEASE)
        .hasMessageContaining("missing tables: uc_storage_cleanup_tasks)");
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);
  }

  @Test
  void makesTextComparisonsCaseSensitive() throws SQLException {
    if (db.kind() == DatabaseKind.MYSQL) {
      // A baseline-release install on MySQL's default collation, which ignores case.
      execute(db, "ALTER DATABASE CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci");
    }
    create07Schema(db);
    // The authorization rules table, shaped like Casbin's adapter creates it.
    execute(
        db, "CREATE TABLE casbin_rule (id INT PRIMARY KEY, ptype VARCHAR(255), v0 VARCHAR(255))");
    execute(db, "INSERT INTO casbin_rule (id, ptype, v0) VALUES (1, 'p', 'Alice')");
    try (Connection connection = db.dataSource().getConnection();
        PreparedStatement insert =
            connection.prepareStatement("INSERT INTO uc_catalogs (id, name) VALUES (?, 'Sales')")) {
      insert.setObject(1, db.kind() == DatabaseKind.MYSQL ? new byte[16] : new UUID(0, 1));
      insert.executeUpdate();
    }

    new SchemaMigrator(db.dataSource(), db.kind()).migrate();

    assertThat(rowsWhere(db, "uc_catalogs", "name", "sales")).isZero();
    assertThat(rowsWhere(db, "uc_catalogs", "name", "Sales")).isOne();
    assertThat(rowsWhere(db, "casbin_rule", "v0", "alice")).isZero();
    assertThat(rowsWhere(db, "casbin_rule", "v0", "Alice")).isOne();
  }

  @Test
  void convertsALatin1DatabaseWithTheStagingLocationIndex() {
    assumeTrue(db.kind() == DatabaseKind.MYSQL);
    // A 0.7.0 server on a latin1 database created idx_staging_location, which is too long in
    // utf8mb4.
    execute(db, "ALTER DATABASE CHARACTER SET latin1 COLLATE latin1_swedish_ci");
    create07Schema(db);
    execute(db, "CREATE INDEX idx_staging_location ON uc_staging_tables (staging_location)");
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());

    migrator.migrate();

    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void rerunsAFailedStepThatIsSafeToRepeat() throws SQLException {
    assumeTrue(db.kind() == DatabaseKind.MYSQL);
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    migrator.migrate();
    // Step 3 converted some tables, then failed.
    execute(
        db, "ALTER TABLE uc_catalogs CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci");
    execute(db, "DELETE FROM uc_schema_history WHERE version = '3'");
    recordFailedStep(db, "V3__case_sensitive_text.sql");

    migrator.migrate();

    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                "SELECT table_collation FROM information_schema.tables"
                    + " WHERE table_schema = DATABASE() AND table_name = 'uc_catalogs'")) {
      rs.next();
      assertThat(rs.getString(1)).isEqualTo("utf8mb4_0900_bin");
    }
  }

  @Test
  void upgradesAVersionedDatabase() {
    String latest = new SchemaMigrator(db.dataSource(), db.kind()).migrate().to();
    List<String> nextRelease = List.of("classpath:db/test-steps/ahead");
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind(), nextRelease);
    assertThat(migrator.state()).isEqualTo(SchemaState.PENDING);

    assertThat(migrator.migrate()).isEqualTo(new MigrationOutcome(latest, "1000", 1));
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void refusesADatabaseOfAnotherApplication() {
    execute(db, "CREATE TABLE orders (id INT)");
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    assertThat(migrator.state()).isEqualTo(SchemaState.NOT_UC);

    assertThatThrownBy(migrator::migrate).hasMessageContaining("not a Unity Catalog database");
  }

  @Test
  void refusesADatabaseWithAnotherApplicationsFlywayHistory() {
    execute(db, "CREATE TABLE flyway_schema_history (installed_rank INT)");
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    assertThat(migrator.state()).isEqualTo(SchemaState.NOT_UC);

    assertThatThrownBy(migrator::migrate).hasMessageContaining("not a Unity Catalog database");
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);
  }

  @Test
  void secondRunAppliesNothing() {
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    migrator.migrate();

    assertThat(migrator.migrate().applied()).isZero();
  }

  @Test
  void refusesASchemaNewerThanThisRelease() {
    new SchemaMigrator(db.dataSource(), db.kind(), List.of("classpath:db/test-steps/ahead"))
        .migrate();
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    assertThat(migrator.state()).isEqualTo(SchemaState.AHEAD);

    assertThatThrownBy(migrator::migrate).hasMessageContaining("newer than this release");
  }

  @Test
  void reportsAFailedStep() {
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    migrator.migrate();
    recordFailedStep(db);

    assertThat(migrator.state()).isEqualTo(SchemaState.FAILED);
  }

  /** Records a failed step in Flyway's history, as a run that failed midway leaves it. */
  static void recordFailedStep(TestDatabase db) {
    recordFailedStep(db, "V1000__broken.sql");
  }

  /** Records the SQL step {@code script} as failed, as a run that failed midway leaves it. */
  static void recordFailedStep(TestDatabase db, String script) {
    String version = script.substring(1, script.indexOf("__"));
    String description =
        script
            .substring(script.indexOf("__") + 2, script.length() - ".sql".length())
            .replace('_', ' ');
    execute(
        db,
        ("INSERT INTO %1$suc_schema_history%1$s (%1$sinstalled_rank%1$s, %1$sversion%1$s,"
                + " %1$sdescription%1$s, %1$stype%1$s, %1$sscript%1$s, %1$sinstalled_by%1$s,"
                + " %1$sexecution_time%1$s, %1$ssuccess%1$s)"
                + " VALUES (1000, '%2$s', '%3$s', 'SQL', '%4$s', 'test', 0, false)")
            .formatted(quote(db), version, description, script));
  }

  @Test
  void concurrentRunsApplyEachStepOnce() throws Exception {
    CyclicBarrier start = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    List<MigrationOutcome> outcomes = new ArrayList<>();
    try {
      List<Future<MigrationOutcome>> runs = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        runs.add(
            executor.submit(
                () -> {
                  start.await();
                  return new SchemaMigrator(db.dataSource(), db.kind()).migrate();
                }));
      }
      for (Future<MigrationOutcome> run : runs) {
        outcomes.add(run.get(2, TimeUnit.MINUTES));
      }
    } finally {
      executor.shutdownNow();
    }

    // The second run waits for the first, then finds the schema current.
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    String latest = migrator.latestVersion();
    int steps = releaseSteps(migrator);
    assertThat(outcomes)
        .containsExactlyInAnyOrder(
            new MigrationOutcome(null, latest, steps), new MigrationOutcome(latest, latest, 0));
    assertThat(historyVersions(db))
        .containsExactlyElementsOf(
            IntStream.rangeClosed(1, steps).mapToObj(String::valueOf).toList());
  }

  /** The number of steps in this release: they are numbered 1, 2, 3 and so on. */
  static int releaseSteps(SchemaMigrator migrator) {
    return Integer.parseInt(migrator.latestVersion());
  }

  /** Builds a database of the baseline release: its schema without Flyway's history. */
  static void create07Schema(TestDatabase db) {
    String path = "db/migration/" + db.kind().folder() + "/V1__baseline_0_7.sql";
    String sql;
    try (InputStream in = MigratorTest.class.getClassLoader().getResourceAsStream(path)) {
      sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    String withoutComments = sql.replaceAll("(?m)^\\s*--.*$", "");
    for (String statement : withoutComments.split(";")) {
      if (!statement.isBlank()) {
        execute(db, statement);
      }
    }
  }

  private static int rowsWhere(TestDatabase db, String table, String column, String value)
      throws SQLException {
    try (Connection connection = db.dataSource().getConnection();
        PreparedStatement query =
            connection.prepareStatement(
                "SELECT COUNT(*) FROM " + table + " WHERE " + column + " = ?")) {
      query.setString(1, value);
      try (ResultSet rs = query.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  /** The versions in Flyway's history (rows without a version left out). */
  private static List<String> historyVersions(TestDatabase db) throws SQLException {
    String q = quote(db);
    List<String> versions = new ArrayList<>();
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                ("SELECT %1$sversion%1$s FROM %1$suc_schema_history%1$s"
                        + " WHERE %1$sversion%1$s IS NOT NULL ORDER BY %1$sversion%1$s")
                    .formatted(q))) {
      while (rs.next()) {
        versions.add(rs.getString(1));
      }
    }
    return versions;
  }

  /** Flyway creates its history table with quoted lower-case names. */
  static String quote(TestDatabase db) {
    try (Connection connection = db.dataSource().getConnection()) {
      return connection.getMetaData().getIdentifierQuoteString();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  static void execute(TestDatabase db, String sql) {
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new IllegalStateException(sql, e);
    }
  }
}
