package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import javax.sql.DataSource;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

public class SchemaMigratorTest {

  /** A test-only step 1000: the next release's step for a current database. */
  private static final List<String> NEXT_RELEASE = List.of("classpath:db/test-steps/ahead");

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void setsUpAnEmptyDatabase() {
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.EMPTY);

    assertThat(migrator.upgradeSchema()).isEqualTo(releaseSteps(migrator));
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void setsUpADatabaseWithOnlyTheAuthorizationRulesTable() {
    UpgradeTestUtils.execute(db, "CREATE TABLE casbin_rule (id INT)");
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.EMPTY);

    assertThat(migrator.upgradeSchema()).isEqualTo(releaseSteps(migrator));
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void setsUpADatabaseBehindAWrappingDriver() {
    Properties wrapped = db.hibernateProperties();
    wrapped.setProperty("hibernate.connection.driver_class", WrappingDriver.class.getName());
    wrapped.setProperty(
        "hibernate.connection.url",
        WrappingDriver.PREFIX + db.jdbcUrl().substring("jdbc:".length()));

    try (SchemaMigrator migrator = SchemaMigrator.fromHibernateProperties(wrapped)) {
      assertThat(migrator.upgradeSchema()).isEqualTo(releaseSteps(migrator));
    }
  }

  @Test
  void setsUpADatabaseOfAPoolConfiguredByDataSourceClass() {
    Properties byClass = db.hibernateProperties();
    byClass.remove("hibernate.connection.url");
    byClass.remove("hibernate.connection.driver_class");
    byClass.setProperty("hibernate.hikari.dataSourceClassName", dataSourceClass());
    byClass.setProperty("hibernate.hikari.dataSource.url", db.jdbcUrl());
    byClass.setProperty(
        "hibernate.hikari.dataSource.user", byClass.getProperty("hibernate.connection.username"));
    byClass.setProperty(
        "hibernate.hikari.dataSource.password",
        byClass.getProperty("hibernate.connection.password"));

    try (SchemaMigrator migrator = SchemaMigrator.fromHibernateProperties(byClass)) {
      assertThat(migrator.upgradeSchema()).isEqualTo(releaseSteps(migrator));
    }
  }

  @Test
  void refusesADatabaseWithoutSchemaHistory() {
    UpgradeTestUtils.createBaselineSchema(db);
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.UNVERSIONED_UC);

    assertThatThrownBy(migrator::upgradeSchema)
        .hasMessageContaining("adopting a database without schema history is not supported yet");
    assertThat(migrator.state()).isEqualTo(SchemaState.UNVERSIONED_UC);
  }

  @Test
  void upgradesAVersionedDatabase() {
    new SchemaMigrator(db.dataSource()).upgradeSchema();
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), NEXT_RELEASE);
    assertThat(migrator.state()).isEqualTo(SchemaState.PENDING);

    assertThat(migrator.upgradeSchema()).isOne();
    assertThat(migrator.recordedVersion()).isEqualTo("1000");
  }

  @Test
  void setsUpADatabaseNextToAnotherApplicationsTables() {
    UpgradeTestUtils.execute(db, "CREATE TABLE orders (id INT)");
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.EMPTY);

    assertThat(migrator.upgradeSchema()).isEqualTo(releaseSteps(migrator));
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
    // The other table is left alone.
    UpgradeTestUtils.execute(db, "SELECT id FROM orders");
  }

  @Test
  void refusesWhenAnotherTablesIndexTookAStepsIndexName() {
    assumeFalse(db.jdbcUrl().startsWith("jdbc:mysql:"), "MySQL names indexes per table");
    UpgradeTestUtils.execute(db, "CREATE TABLE other_app (x INT)");
    UpgradeTestUtils.execute(db, "CREATE INDEX idx_name ON other_app (x)");

    assertThatThrownBy(() -> new SchemaMigrator(db.dataSource()).upgradeSchema())
        .hasMessageMatching("(?is).*index idx_name is used by table other_app.*")
        .hasMessageContaining("restore the backup")
        .hasMessageContaining("rename that index");

    // Renaming the other index does not create ours: a rerun still refuses.
    UpgradeTestUtils.execute(db, "ALTER INDEX idx_name RENAME TO other_app_x");
    assertThatThrownBy(() -> new SchemaMigrator(db.dataSource()).upgradeSchema())
        .hasMessageContaining("index idx_name missing on uc_tables");
  }

  @Test
  void createsOpenSharingsTables() {
    new SchemaMigrator(db.dataSource()).upgradeSchema();
    for (String table :
        List.of(
            "os_shares",
            "os_share_properties",
            "os_shared_data_objects",
            "os_share_permissions",
            "os_recipients",
            "os_recipient_tokens")) {
      // Each table exists and is empty.
      UpgradeTestUtils.execute(db, "SELECT 1 FROM " + table + " WHERE 1 = 0");
    }
  }

  @Test
  void doesNotBlameAnOpenSharingTableForAStepsIndexName() {
    assumeFalse(db.jdbcUrl().startsWith("jdbc:mysql:"), "MySQL names indexes per table");
    UpgradeTestUtils.execute(db, "CREATE TABLE os_other (x INT)");
    UpgradeTestUtils.execute(db, "CREATE INDEX idx_name ON os_other (x)");

    // os_* tables are Unity Catalog's own, so the index is not "another application's".
    assertThatThrownBy(() -> new SchemaMigrator(db.dataSource()).upgradeSchema())
        .hasMessageContaining("index idx_name missing on uc_tables")
        .hasMessageNotContaining("is used by table");
  }

  @Test
  void readsOnlyTheConfiguredSchema() {
    assumeFalse(db.jdbcUrl().startsWith("jdbc:mysql:"), "MySQL has no schemas within a database");
    // In a metadata search, "_" matches any character: uc_a would also list ucxa's tables.
    UpgradeTestUtils.execute(db, "CREATE SCHEMA uc_a");
    UpgradeTestUtils.execute(db, "CREATE SCHEMA ucxa");
    UpgradeTestUtils.execute(db, "CREATE TABLE ucxa.uc_orders (id INT)");
    HikariConfig config = new HikariConfig();
    config.setDataSource(db.dataSource());
    config.setSchema(isH2() ? "UC_A" : "uc_a");
    try (HikariDataSource onUcA = new HikariDataSource(config)) {
      assertThat(new SchemaMigrator(onUcA).state()).isEqualTo(SchemaState.EMPTY);
    }
  }

  @Test
  void refusesASchemaNewerThanThisRelease() {
    new SchemaMigrator(db.dataSource(), NEXT_RELEASE).upgradeSchema();
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.AHEAD);

    assertThatThrownBy(migrator::upgradeSchema).hasMessageContaining("newer than this release");
  }

  @ParameterizedTest
  @CsvSource({
    "jdbc:h2:file:{dir}/uc;AUTO_SERVER=TRUE,,",
    "jdbc:h2:file:{dir}/uc;AUTO_SERVER=1,,",
    "jdbc:h2:file:{dir}/uc;auto_server=yes,,",
    "jdbc:h2:file:{dir}/uc;AUTO\\_SERVER=TRUE,,",
    "jdbc:h2:tcp://localhost/{dir}/uc,,",
    "jdbc:h2:ssl://localhost/{dir}/uc,,",
    "jdbc:p6spy:h2:file:{dir}/uc;AUTO_SERVER=TRUE,,",
    "jdbc:h2:file:{dir}/uc,hibernate.hikari.dataSource.AUTO_SERVER,TRUE",
    "jdbc:h2:file:{dir}/uc,hibernate.hikari.dataSource.URL,jdbc:h2:tcp://localhost/{dir}/uc",
    ",hibernate.hikari.dataSource.URL,jdbc:h2:file:{dir}/uc;AUTO_SERVER=TRUE",
  })
  void refusesH2InServerMode(String url, String key, String value, @TempDir Path dir) {
    Properties properties = db.hibernateProperties();
    if (url == null) {
      // A pool set up by data source class: the URL is a data source property.
      properties.remove("hibernate.connection.url");
      properties.remove("hibernate.connection.driver_class");
      properties.setProperty("hibernate.hikari.dataSourceClassName", "org.h2.jdbcx.JdbcDataSource");
    } else {
      properties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
      properties.setProperty("hibernate.connection.url", url.replace("{dir}", dir.toString()));
      if (key != null && key.endsWith(".URL")) {
        // Hikari then uses the data source class's URL, not the one above.
        properties.setProperty(
            "hibernate.hikari.dataSourceClassName", "org.h2.jdbcx.JdbcDataSource");
      }
    }
    if (key != null) {
      properties.setProperty(key, value.replace("{dir}", dir.toString()));
    }

    assertThatThrownBy(() -> SchemaMigrator.fromHibernateProperties(properties))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("H2 in server mode is not supported");
  }

  @Test
  void acceptsH2WithAutoServerOff(@TempDir Path dir) {
    Properties properties = db.hibernateProperties();
    properties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    properties.setProperty(
        "hibernate.connection.url", "jdbc:h2:file:" + dir + "/uc;AUTO_SERVER=OFF");

    try (SchemaMigrator migrator = SchemaMigrator.fromHibernateProperties(properties)) {
      assertThat(migrator.state()).isEqualTo(SchemaState.EMPTY);
    }
  }

  @Test
  void refusesPostgresqlWithoutACurrentSchema() {
    assumeFalse(!db.jdbcUrl().startsWith("jdbc:postgresql:"), "only PostgreSQL");
    HikariConfig config = new HikariConfig();
    config.setDataSource(db.dataSource());
    config.setSchema("no_such_schema");
    try (HikariDataSource noSchema = new HikariDataSource(config)) {
      assertThatThrownBy(() -> new SchemaMigrator(noSchema))
          .hasMessageContaining("The connection has no current schema")
          .hasMessageContaining("currentSchema");
    }
  }

  @Test
  void refusesUnityCatalogTablesThatTheSearchPathReachesLater() {
    assumeFalse(!db.jdbcUrl().startsWith("jdbc:postgresql:"), "only PostgreSQL");
    // The tables are in public; then a schema named after the user, first on the search_path,
    // becomes the current schema. Steps run there would hide the tables in public.
    UpgradeTestUtils.createBaselineSchema(db);
    UpgradeTestUtils.execute(
        db, "DO $$ BEGIN EXECUTE format('CREATE SCHEMA %I', current_user); END $$");

    assertThatThrownBy(() -> new SchemaMigrator(db.dataSource()))
        .hasMessageContaining("Unity Catalog tables are in schema public")
        .hasMessageContaining("set currentSchema in the JDBC URL");
    // A schema history alone, as a run that failed in its first step leaves, is no install.
    UpgradeTestUtils.execute(
        db,
        "DO $$ BEGIN EXECUTE format('CREATE TABLE %I.uc_schema_history (id INT)', current_user);"
            + " END $$");
    assertThatThrownBy(() -> new SchemaMigrator(db.dataSource()))
        .hasMessageContaining("Unity Catalog tables are in schema public");
  }

  @Test
  void upgradesTheCurrentSchemaWhenALaterSchemaHasAnOldCopy() {
    assumeFalse(!db.jdbcUrl().startsWith("jdbc:postgresql:"), "only PostgreSQL");
    // Unity Catalog is in the schema named after the user, first on the search_path; public keeps
    // an old copy of one of its tables.
    UpgradeTestUtils.execute(
        db, "DO $$ BEGIN EXECUTE format('CREATE SCHEMA %I', current_user); END $$");
    new SchemaMigrator(db.dataSource()).upgradeSchema();
    UpgradeTestUtils.execute(db, "CREATE TABLE public.uc_tables (id INT)");

    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.upgradeSchema()).isZero();
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void runsStartingAtOnceOnAnEmptyDatabaseTakeTurns() throws Exception {
    assumeFalse(isH2(), "H2 has no run lock");
    // The test holds the run lock, so both runs must wait for it before they change anything.
    CountDownLatch waitingInTheDatabase = new CountDownLatch(2);
    DataSource watched =
        withLockCall(
            SchemaMigratorTest::isBlockingLock,
            sql -> {
              waitingInTheDatabase.countDown();
              return sql;
            });
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      List<Future<Integer>> runs = new ArrayList<>();
      try (SchemaMigrator.RunLock held =
          SchemaMigrator.RunLock.acquire(db.dataSource(), DatabaseKind.of(db.dataSource()))) {
        for (int i = 0; i < 2; i++) {
          runs.add(executor.submit(() -> new SchemaMigrator(watched).upgradeSchema()));
        }
        assertThat(waitingInTheDatabase.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.EMPTY);
      }

      int applied = 0;
      for (Future<Integer> run : runs) {
        applied += run.get(1, TimeUnit.MINUTES);
      }
      SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
      assertThat(applied).isEqualTo(releaseSteps(migrator));
      assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void refusesADatabaseWithAFailedStep() {
    new SchemaMigrator(db.dataSource()).upgradeSchema();
    // What a run that failed in step 2 leaves behind on H2 and MySQL.
    UpgradeTestUtils.execute(
        db,
        "UPDATE %1$suc_schema_history%1$s SET %1$ssuccess%1$s = false WHERE %1$sversion%1$s = '2'"
            .formatted(identifierQuote(db.dataSource())));

    assertThatThrownBy(new SchemaMigrator(db.dataSource())::upgradeSchema)
        .hasMessageContaining("schema history does not match")
        .hasMessageContaining("Detected failed migration to version 2");
  }

  @ParameterizedTest
  @EnumSource(RefusedLock.class)
  void stopsWhenTheRunLockIsRefused(RefusedLock refused) {
    assumeFalse(isH2(), "H2 has no run lock");
    DataSource refusing =
        switch (refused) {
          case CALL_FAILS -> withLockCall(SchemaMigratorTest::isTryLock, sql -> null);
          case NOT_GRANTED ->
              withLockCall(
                  sql -> isTryLock(sql) || isBlockingLock(sql),
                  sql -> "SELECT " + (isTryLock(sql) ? "false" : "NULL"));
        };

    AbstractThrowableAssert<?, ? extends Throwable> thrown =
        assertThatThrownBy(new SchemaMigrator(refusing)::upgradeSchema)
            .hasMessageContaining("Could not take the schema migration lock");
    if (refused == RefusedLock.NOT_GRANTED) {
      thrown.hasMessageContaining("returned NULL");
    }
    assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.EMPTY);
  }

  /** How the database refuses the run lock. */
  private enum RefusedLock {
    /** The lock call fails. */
    CALL_FAILS,
    /** The lock is not granted: the blocking call returns NULL (on MySQL: interrupted). */
    NOT_GRANTED
  }

  @Test
  void stopsWhenTheLockIsNotReleased() {
    assumeFalse(isH2(), "H2 has no run lock");
    DataSource failing =
        withLockCall(
            sql -> sql.contains("pg_advisory_unlock(") || sql.contains("RELEASE_LOCK("),
            sql -> null);

    assertThatThrownBy(new SchemaMigrator(failing)::upgradeSchema)
        .hasMessageContaining("Could not release the schema migration lock");
    // The lock's database session was closed, so a run on other connections takes it at once.
    try (HikariDataSource other = new HikariDataSource()) {
      Properties properties = db.hibernateProperties();
      other.setJdbcUrl(db.jdbcUrl());
      other.setUsername(properties.getProperty("hibernate.connection.username"));
      other.setPassword(properties.getProperty("hibernate.connection.password"));
      assertThat(
              assertTimeoutPreemptively(
                  Duration.ofSeconds(30), () -> new SchemaMigrator(other).upgradeSchema()))
          .isZero();
    }
  }

  /** The number of steps in this release: they are numbered 1, 2, 3 and so on. */
  static int releaseSteps(SchemaMigrator migrator) {
    return Integer.parseInt(migrator.latestVersion());
  }

  /** The DataSource class of {@code db}'s database. */
  private String dataSourceClass() {
    return switch (DatabaseKind.of(db.dataSource())) {
      case H2 -> "org.h2.jdbcx.JdbcDataSource";
      case POSTGRESQL -> "org.postgresql.ds.PGSimpleDataSource";
      case MYSQL -> "com.mysql.cj.jdbc.MysqlDataSource";
    };
  }

  private boolean isH2() {
    return db.jdbcUrl().startsWith("jdbc:h2:");
  }

  private static boolean isTryLock(String sql) {
    return sql.contains("pg_try_advisory_lock(")
        || (sql.contains("GET_LOCK(") && sql.endsWith(", 0)"));
  }

  private static boolean isBlockingLock(String sql) {
    return sql.contains("pg_advisory_lock(")
        || (sql.contains("GET_LOCK(") && sql.endsWith(", -1)"));
  }

  /**
   * {@code db}'s data source, but a run lock call that {@code matches} runs {@code rewrite}'s SQL
   * instead, or fails when that is {@code null}.
   */
  private DataSource withLockCall(Predicate<String> matches, UnaryOperator<String> rewrite) {
    return UpgradeTestUtils.watched(
        db.dataSource(),
        (sql, prepared) -> {
          if (prepared || !matches.test(sql)) {
            return sql;
          }
          String rewritten = rewrite.apply(sql);
          if (rewritten == null) {
            throw new SQLException("Test: the lock call failed");
          }
          return rewritten;
        });
  }

  /** The quote for identifiers: Flyway's history table has quoted lower-case column names. */
  private static String identifierQuote(DataSource dataSource) {
    try (Connection connection = dataSource.getConnection()) {
      return connection.getMetaData().getIdentifierQuoteString();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /** A driver that wraps the real one, like P6Spy's or AWS's: {@code jdbc:wrapper:h2:...}. */
  public static class WrappingDriver implements Driver {
    static final String PREFIX = "jdbc:wrapper:";

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      return acceptsURL(url)
          ? DriverManager.getConnection("jdbc:" + url.substring(PREFIX.length()), info)
          : null;
    }

    @Override
    public boolean acceptsURL(String url) {
      return url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
      return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
      return 1;
    }

    @Override
    public int getMinorVersion() {
      return 0;
    }

    @Override
    public boolean jdbcCompliant() {
      return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }
  }
}
