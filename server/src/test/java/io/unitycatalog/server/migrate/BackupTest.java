package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.migrate.MigratorTest.execute;
import static io.unitycatalog.server.migrate.MigratorTest.quote;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.migrate.SchemaMigrator.MigrationOutcome;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.VersionUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The backup before the upgrade ({@code server.migration.backup}). A test-only release map makes
 * version 6000 (a test-only step that creates {@code test_backup_items}) the last step of the
 * release before this one. This release's test-only steps add a column to it (6001) and fail on a
 * missing table (6002).
 */
public class BackupTest {

  private static final String BASE = "classpath:db/test-steps/backup-base/";
  private static final String ADD = "classpath:db/test-steps/backup-add/";
  private static final String FAIL = "classpath:db/test-steps/backup-fail/";
  private static final String DRIFT = "classpath:db/test-steps/backup-drift/";

  private static final Properties RELEASES = SkippedReleaseTest.releases("0.7.0=1", "0.7.5=6000");

  private static final String RELEASE = VersionUtils.VERSION.replaceFirst("-.*", "");

  /** MySQL: the database's tables. */
  private static final String TABLES =
      "SELECT table_name FROM information_schema.tables"
          + " WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'";

  /** The key of {@code SchemaMigrator.RunLock}'s PostgreSQL advisory lock. */
  private static final long RUN_LOCK_KEY = 0x75635f6d69677261L;

  private static final String RESTORED =
      "The database was restored from the backup taken before the upgrade; the previous release"
          + " can start on it.";

  @TempDatabase(empty = true)
  TestDatabase db;

  /** A database of the release before this one, with two rows in the test table. */
  @BeforeEach
  void migrateToThePreviousRelease() {
    new SchemaMigrator(db.dataSource(), db.kind(), List.of(BASE + db.kind().folder())).migrate();
    execute(db, "INSERT INTO test_backup_items (id, name) VALUES (1, 'one'), (2, 'two')");
  }

  @AfterEach
  void dropCompanions() {
    dropCompanions(db);
  }

  @Test
  void restoresTheDatabaseWhenAStepFails() {
    Contents before = contents();

    assertThatThrownBy(migrator(null, ADD, FAIL)::migrate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("V6002__fail_on_a_missing_table.sql")
        .hasMessageEndingWith(RESTORED);
    assertThat(contents()).isEqualTo(before);
    assertThat(backupExists()).isFalse();
    assertThat(marks()).isEmpty();
    if (db.kind() == DatabaseKind.MYSQL) {
      // The moved tables' foreign keys point at the moved tables, not at the dropped copy's.
      assertThat(
              rows(
                  db,
                  "SELECT DISTINCT referenced_table_schema FROM information_schema.key_column_usage"
                      + " WHERE table_schema = DATABASE() AND referenced_table_name IS NOT NULL"))
          .containsExactly(name(db));
    }
  }

  @Test
  void restoresTheDatabaseWhenTheSchemaCheckFails() {
    Contents before = contents();
    SchemaMigrator migrator =
        new SchemaMigrator(
                db.dataSource(),
                db.kind(),
                List.of(BASE + db.kind().folder(), DRIFT + db.kind().folder()))
            .withBackup(null);

    assertThatThrownBy(migrator::migrate)
        .hasMessageContaining("The schema of uc_function_params differs")
        .hasMessageEndingWith(RESTORED);
    assertThat(contents()).isEqualTo(before);
    assertThat(backupExists()).isFalse();
  }

  @Test
  void dropsTheBackupAfterASuccessfulUpgrade() {
    assertThat(migrator(null, ADD).migrate()).isEqualTo(new MigrationOutcome("6000", "6001", 1));

    assertThat(backupExists()).isFalse();
  }

  /** A backup that a run killed midway left: the rerun restores it, not the half-upgraded one. */
  @Test
  void reusesTheBackupWhileMidUpgrade() {
    Contents before = contents();
    new Backup(db.dataSource(), db.kind(), SchemaMigrator.LOCK_WAIT, null).take(false);
    assertThatThrownBy(withoutBackup(ADD, FAIL)::migrate)
        .hasMessageContaining("V6002__fail_on_a_missing_table.sql");

    assertThatThrownBy(migrator(null, ADD, FAIL)::migrate).hasMessageEndingWith(RESTORED);
    assertThat(contents()).isEqualTo(before);
  }

  /** An older backup is replaced: the restore keeps the rows written after it. */
  @Test
  void takesAFreshBackupAtThePreviousRelease() {
    new Backup(db.dataSource(), db.kind(), SchemaMigrator.LOCK_WAIT, null).take(false);
    execute(db, "INSERT INTO test_backup_items (id, name) VALUES (3, 'three')");
    Contents before = contents();

    assertThatThrownBy(migrator(null, ADD, FAIL)::migrate).hasMessageEndingWith(RESTORED);
    assertThat(contents()).isEqualTo(before);
  }

  /**
   * MySQL: a restore that fails partway, here on a table a client's open transaction holds, leaves
   * the database as it was and the copy in place.
   */
  @Test
  void failedRestoreChangesNothing() throws SQLException {
    assumeTrue(db.kind() == DatabaseKind.MYSQL);
    Backup backup = new Backup(db.dataSource(), db.kind(), Duration.ofSeconds(1), null);
    backup.take(false);
    execute(db, "INSERT INTO test_backup_items (id, name) VALUES (3, 'three')");
    Contents changed = contents();
    List<String> tables = rows(db, TABLES);
    try (Connection client = db.dataSource().getConnection();
        Statement statement = client.createStatement()) {
      client.setAutoCommit(false);
      // The restore reaches the table it lists last after the others.
      statement.executeQuery("SELECT * FROM " + tables.get(tables.size() - 1)).close();

      assertThatThrownBy(backup::restore).hasMessageContaining("Could not restore the backup");
      client.rollback();
    }
    assertThat(contents()).isEqualTo(changed);
    assertThat(backupExists()).isTrue();
  }

  /** MySQL: a copy whose tables the database lacks is kept, as the only good one, and refused. */
  @Test
  void keepsTheBackupOfTablesTheDatabaseLacks() {
    assumeTrue(db.kind() == DatabaseKind.MYSQL);
    new Backup(db.dataSource(), db.kind(), SchemaMigrator.LOCK_WAIT, null).take(false);
    execute(db, "DROP TABLE test_backup_items");

    assertThatThrownBy(migrator(null, ADD)::migrate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(name(db) + "_uc_backup")
        .hasMessageContaining("test_backup_items");
    assertThat(backupExists()).isTrue();
  }

  /**
   * MySQL: a copy that a run killed while copying left (its tables without their rows) is no
   * backup; a run partway through the steps refuses instead of restoring it.
   */
  @Test
  void refusesAnIncompleteBackupWhileMidUpgrade() {
    assumeTrue(db.kind() == DatabaseKind.MYSQL);
    assertThatThrownBy(withoutBackup(ADD, FAIL)::migrate)
        .hasMessageContaining("V6002__fail_on_a_missing_table.sql");
    List<String> items = rows(db, "SELECT * FROM test_backup_items ORDER BY id");
    String copy = name(db) + "_uc_backup";
    execute(db, "CREATE DATABASE `" + copy + "`");
    for (String table : rows(db, TABLES)) {
      execute(db, "CREATE TABLE `%s`.`%2$s` LIKE `%2$s`".formatted(copy, table));
    }

    assertThatThrownBy(migrator(null, ADD, FAIL)::migrate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("The backup " + copy + " is incomplete");
    assertThat(rows(db, "SELECT * FROM test_backup_items ORDER BY id")).isEqualTo(items);
  }

  /**
   * PostgreSQL: the restored database keeps the database's own settings, grants and connection
   * limit, which {@code CREATE DATABASE ... TEMPLATE} doesn't copy.
   */
  @Test
  void restoreKeepsTheDatabaseSettingsAndGrants() {
    assumeTrue(db.kind() == DatabaseKind.POSTGRESQL);
    String database = "\"" + name(db) + "\"";
    String role = "\"" + name(db) + "_reader\"";
    execute(db, "CREATE ROLE " + role);
    try {
      execute(db, "ALTER DATABASE " + database + " SET search_path = public, \"$user\"");
      execute(db, "ALTER DATABASE " + database + " SET application_name = 'it''s kept'");
      execute(db, "ALTER ROLE " + role + " IN DATABASE " + database + " SET work_mem = '8MB'");
      execute(db, "GRANT CONNECT ON DATABASE " + database + " TO " + role);
      execute(db, "ALTER DATABASE " + database + " CONNECTION LIMIT 50");
      List<String> before = databaseAttributes();

      assertThatThrownBy(migrator(null, ADD, FAIL)::migrate).hasMessageEndingWith(RESTORED);
      assertThat(databaseAttributes()).isEqualTo(before);
    } finally {
      dropCompanions(db);
      execute(db, "DROP OWNED BY " + role); // its grant on the database
      execute(db, "DROP ROLE " + role);
    }
  }

  /** PostgreSQL: the database's settings by role, ACL, owner and connection limit. */
  private List<String> databaseAttributes() {
    return rows(
        db,
        "SELECT (SELECT string_agg(COALESCE(r.rolname, '') || ':' || array_to_string(s.setconfig,"
            + " '|'), ';' ORDER BY r.rolname NULLS FIRST) FROM pg_db_role_setting s LEFT JOIN"
            + " pg_roles r ON r.oid = s.setrole WHERE s.setdatabase = d.oid),"
            + " (SELECT string_agg(a::text, ';' ORDER BY a::text) FROM unnest(d.datacl) a),"
            + " pg_get_userbyid(d.datdba), d.datconnlimit"
            + " FROM pg_database d WHERE d.datname = current_database()");
  }

  @Test
  void refusesTheChartRevisionThatRestored() {
    assertThatThrownBy(migrator("7", ADD, FAIL)::migrate).hasMessageEndingWith(RESTORED);
    Contents restored = contents();
    assertThat(marks()).containsExactly(RELEASE + " 7");

    assertThatThrownBy(migrator("7", ADD)::migrate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The upgrade to "
                + RELEASE
                + " failed and the database was restored in chart revision 7; fix the cause,"
                + " then run helm upgrade again (a new revision clears this).");
    assertThat(contents()).isEqualTo(restored);

    assertThat(migrator("8", ADD).migrate().to()).isEqualTo("6001");
    assertThat(marks()).isEmpty();
  }

  /** PostgreSQL: the companion database has no schema of the URL's {@code currentSchema}. */
  @Test
  void keepsTheMarkWithASchemaInTheUrl() {
    assumeTrue(db.kind() == DatabaseKind.POSTGRESQL);
    execute(db, "CREATE SCHEMA uc_test");
    HikariDataSource pool = (HikariDataSource) db.dataSource();
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(db.jdbcUrl() + "?currentSchema=uc_test");
    config.setUsername(pool.getUsername());
    config.setPassword(pool.getPassword());
    try (HikariDataSource withSchema = new HikariDataSource(config)) {
      Backup backup = new Backup(withSchema, db.kind(), SchemaMigrator.LOCK_WAIT, "7");
      backup.checkBeforeRun(RELEASE);
      backup.markRestored(RELEASE);

      assertThatThrownBy(() -> backup.checkBeforeRun(RELEASE))
          .hasMessageContaining("restored in chart revision 7");
    }
  }

  /** PostgreSQL: a run waits for another run's lock past the lock-wait cap, however long. */
  @Test
  void waitsForTheRunLockPastTheLockWait() throws Exception {
    assumeTrue(db.kind() == DatabaseKind.POSTGRESQL);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Connection holder = companionConnection();
        Statement statement = holder.createStatement()) {
      statement.execute("SELECT pg_advisory_lock(" + RUN_LOCK_KEY + ")");
      List<String> locations = List.of(BASE + db.kind().folder(), ADD + db.kind().folder());
      Future<MigrationOutcome> run =
          executor.submit(
              () ->
                  new SchemaMigrator(db.dataSource(), db.kind(), locations, Duration.ofSeconds(1))
                      .withBackup(null)
                      .migrate());
      Thread.sleep(3000);
      assertThat(run.isDone()).as("the run still waits").isFalse();

      statement.execute("SELECT pg_advisory_unlock(" + RUN_LOCK_KEY + ")");
      assertThat(run.get(1, TimeUnit.MINUTES).to()).isEqualTo("6001");
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * PostgreSQL: {@code bin/start-uc-server --check-schema} with the backup on takes the run lock
   * where a start with the backup on does, so it waits while that start runs.
   */
  @Test
  void checkSchemaWaitsForARunWithTheBackupOn() throws Exception {
    assumeTrue(db.kind() == DatabaseKind.POSTGRESQL);
    Path directory = Files.createTempDirectory("uc-check-schema");
    Path conf = Files.createDirectories(directory.resolve("etc/conf"));
    try (Writer out = Files.newBufferedWriter(conf.resolve("hibernate.properties"))) {
      db.hibernateProperties().store(out, null);
    }
    String waiting =
        "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()"
            + " AND query LIKE 'SELECT pg_advisory_lock(%' AND pid <> pg_backend_pid()";
    Process check = null;
    try (Connection holder = companionConnection();
        Statement statement = holder.createStatement()) {
      statement.execute("SELECT pg_advisory_lock(" + RUN_LOCK_KEY + ")");
      check =
          new ProcessBuilder(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "-Djacoco-agent.output=none",
                  "-D" + Property.MIGRATION_BACKUP.getKey() + "=true",
                  "-cp",
                  System.getProperty("java.class.path"),
                  UnityCatalogServer.class.getName(),
                  "--check-schema")
              .directory(directory.toFile())
              .inheritIO()
              .start();
      long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
      while (count(statement, waiting) == 0) {
        assertThat(check.isAlive()).as("--check-schema waits for the lock").isTrue();
        assertThat(System.nanoTime()).as("--check-schema started in time").isLessThan(deadline);
        Thread.sleep(100);
      }
      statement.execute("SELECT pg_advisory_unlock(" + RUN_LOCK_KEY + ")");
      assertThat(check.waitFor(2, TimeUnit.MINUTES)).isTrue();
    } finally {
      if (check != null) {
        check.destroyForcibly();
      }
      Files.delete(conf.resolve("hibernate.properties"));
      Files.delete(conf);
      Files.delete(conf.getParent());
      Files.delete(directory);
    }
  }

  /** PostgreSQL: a connection of its own to the companion database, which this creates. */
  private Connection companionConnection() throws SQLException {
    return new Backup(db.dataSource(), db.kind(), SchemaMigrator.LOCK_WAIT, null)
        .companionConnection();
  }

  private static int count(Statement statement, String sql) throws SQLException {
    try (ResultSet rs = statement.executeQuery(sql)) {
      rs.next();
      return rs.getInt(1);
    }
  }

  @Test
  void checkSchemaReportsTheRestoredMark() {
    assertThatThrownBy(migrator("7", ADD, FAIL)::migrate).hasMessageEndingWith(RESTORED);
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    assertThat(migrator("7", ADD).checkSchema(new PrintStream(out, true, UTF_8))).isOne();
    assertThat(out.toString(UTF_8)).contains("restored in chart revision 7");
    assertThat(marks()).containsExactly(RELEASE + " 7");
  }

  /** H2: an empty mark file, as a crash while writing it leaves, is no mark. */
  @Test
  void ignoresAnEmptyMarkFile() throws IOException {
    assumeTrue(db.kind() == DatabaseKind.H2);
    Files.createFile(Path.of(name(db) + "_uc_upgrade.properties"));

    assertThat(migrator("7", ADD).migrate().to()).isEqualTo("6001");
  }

  @Test
  void refusesWhileAnotherSessionIsConnected() throws SQLException {
    assumeTrue(db.kind() == DatabaseKind.POSTGRESQL);
    Contents before = contents();
    HikariDataSource pool = (HikariDataSource) db.dataSource();
    try (Connection other =
        DriverManager.getConnection(db.jdbcUrl(), pool.getUsername(), pool.getPassword())) {
      assertThatThrownBy(migrator(null, ADD)::migrate)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("other sessions")
          .hasMessageContaining("Stop the other servers");
    }
    assertThat(contents()).isEqualTo(before);
    assertThat(backupExists()).isFalse();
  }

  @Test
  void refusesAnInMemoryH2Database() {
    assumeTrue(db.kind() == DatabaseKind.H2);
    try (TestDatabase memory = TestDatabases.newDatabase()) {
      SchemaMigrator migrator =
          new SchemaMigrator(memory.dataSource(), memory.kind()).withBackup(null);

      assertThatThrownBy(migrator::migrate)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("an H2 file database");
    }
  }

  /** This release's test-only steps {@code steps}, with the backup on in {@code chartRevision}. */
  private SchemaMigrator migrator(String chartRevision, String... steps) {
    return withoutBackup(steps).withBackup(chartRevision);
  }

  private SchemaMigrator withoutBackup(String... steps) {
    List<String> locations = new ArrayList<>();
    locations.add(BASE + db.kind().folder());
    for (String step : steps) {
      locations.add(step + db.kind().folder());
    }
    return new SchemaMigrator(db.dataSource(), db.kind(), locations, RELEASES);
  }

  /** What a restore must bring back: the schema, the test table's rows and the schema history. */
  record Contents(SchemaSnapshot schema, List<String> items, List<String> history) {}

  private Contents contents() {
    String q = quote(db);
    return new Contents(
        SchemaSnapshot.of(db.dataSource()),
        rows(db, "SELECT * FROM test_backup_items ORDER BY id"),
        rows(
            db,
            "SELECT * FROM %1$suc_schema_history%1$s ORDER BY %1$sinstalled_rank%1$s"
                .formatted(q)));
  }

  /** Each row of {@code sql} as its values joined with commas. */
  static List<String> rows(TestDatabase db, String sql) {
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      List<String> rows = new ArrayList<>();
      while (rs.next()) {
        List<String> values = new ArrayList<>();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
          values.add(rs.getString(i));
        }
        rows.add(String.join(",", values));
      }
      return rows;
    } catch (SQLException e) {
      throw new IllegalStateException(sql, e);
    }
  }

  /** The database's name (PostgreSQL, MySQL) or file path (H2). */
  private static String name(TestDatabase db) {
    String sql =
        switch (db.kind()) {
          case POSTGRESQL -> "SELECT current_database()";
          case MYSQL -> "SELECT DATABASE()";
          case H2 -> "SELECT DATABASE_PATH()";
        };
    return rows(db, sql).get(0);
  }

  private boolean backupExists() {
    String backup = name(db) + "_uc_backup";
    return switch (db.kind()) {
      case POSTGRESQL ->
          !rows(db, "SELECT 1 FROM pg_database WHERE datname = '" + backup + "'").isEmpty();
      case MYSQL ->
          !rows(
                  db,
                  "SELECT 1 FROM information_schema.schemata WHERE schema_name = '" + backup + "'")
              .isEmpty();
      case H2 -> Files.exists(Path.of(backup + ".mv.db"));
    };
  }

  /** The restored marks, as {@code <release> <revision>}. */
  private List<String> marks() {
    String companion = name(db) + "_uc_upgrade";
    return switch (db.kind()) {
      case H2 -> h2Marks(Path.of(companion + ".properties"));
      case MYSQL ->
          rows(
                      db,
                      "SELECT 1 FROM information_schema.tables WHERE table_schema = '"
                          + companion
                          + "' AND table_name = 'uc_restored'")
                  .isEmpty()
              ? List.of()
              : rows(
                  db, "SELECT CONCAT(`release`, ' ', revision) FROM " + companion + ".uc_restored");
      case POSTGRESQL -> postgresMarks(companion);
    };
  }

  private static List<String> h2Marks(Path file) {
    if (!Files.exists(file)) {
      return List.of();
    }
    Properties mark = new Properties();
    try (Reader in = Files.newBufferedReader(file)) {
      mark.load(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return List.of(mark.getProperty("release") + " " + mark.getProperty("revision"));
  }

  private List<String> postgresMarks(String companion) {
    HikariDataSource pool = (HikariDataSource) db.dataSource();
    String url = db.jdbcUrl().replaceFirst("/[^/]+$", "/" + companion);
    List<String> marks = new ArrayList<>();
    try (Connection connection =
            DriverManager.getConnection(url, pool.getUsername(), pool.getPassword());
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery("SELECT release || ' ' || revision FROM uc_restored")) {
      while (rs.next()) {
        marks.add(rs.getString(1));
      }
    } catch (SQLException e) {
      if (!"42P01".equals(e.getSQLState()) && !"3D000".equals(e.getSQLState())) {
        throw new IllegalStateException(e); // not a missing table or database
      }
    }
    return marks;
  }

  /**
   * Drops the backup copy, the companion database and MySQL's trash that a run with the backup on
   * may leave on the database server (PostgreSQL, MySQL; H2's files go with the test's folder).
   */
  static void dropCompanions(TestDatabase db) {
    if (db.kind() == DatabaseKind.H2) {
      return;
    }
    String name = name(db);
    for (String suffix : List.of("_uc_backup", "_uc_upgrade", "_uc_trash")) {
      execute(
          db,
          db.kind() == DatabaseKind.POSTGRESQL
              ? "DROP DATABASE IF EXISTS \"" + name + suffix + "\" WITH (FORCE)"
              : "DROP DATABASE IF EXISTS `" + name + suffix + "`");
    }
  }
}
