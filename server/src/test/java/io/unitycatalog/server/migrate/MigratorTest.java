package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import javax.sql.DataSource;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class MigratorTest {

  /** A test-only step 1000: the next release's step for a current database. */
  private static final List<String> NEXT_RELEASE = List.of("classpath:db/test-steps/ahead");

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void setsUpAnEmptyDatabase() {
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.EMPTY);

    assertThat(migrator.migrate()).isEqualTo(releaseSteps(migrator));
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void setsUpADatabaseWithOnlyTheAuthorizationRulesTable() {
    UpgradeTestUtils.execute(db, "CREATE TABLE casbin_rule (id INT)");
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.EMPTY);

    assertThat(migrator.migrate()).isEqualTo(releaseSteps(migrator));
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
      assertThat(migrator.migrate()).isEqualTo(releaseSteps(migrator));
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
      assertThat(migrator.migrate()).isEqualTo(releaseSteps(migrator));
    }
  }

  @Test
  void adoptsA07Database() {
    UpgradeTestUtils.create07Schema(db);
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.UNVERSIONED_UC);

    // Version 1 is recorded as the baseline, not run; the steps after it run.
    assertThat(migrator.migrate()).isEqualTo(releaseSteps(migrator) - 1);
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void refusesADatabaseFromBeforeTheBaselineRelease() {
    UpgradeTestUtils.create07Schema(db);
    UpgradeTestUtils.execute(db, "DROP TABLE uc_storage_cleanup_tasks");
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());

    assertThatThrownBy(new SchemaMigrator(db.dataSource())::migrate)
        .hasMessageContaining("from a release before " + SchemaMigrator.BASELINE_RELEASE)
        .hasMessageContaining("missing tables: uc_storage_cleanup_tasks)");
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);
  }

  @Test
  void upgradesAVersionedDatabase() {
    new SchemaMigrator(db.dataSource()).migrate();
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), NEXT_RELEASE);
    assertThat(migrator.state()).isEqualTo(SchemaState.PENDING);

    assertThat(migrator.migrate()).isOne();
    assertThat(migrator.currentVersion()).isEqualTo("1000");
  }

  @Test
  void refusesADatabaseOfAnotherApplication() {
    UpgradeTestUtils.execute(db, "CREATE TABLE orders (id INT)");
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.NOT_UC);

    assertThatThrownBy(migrator::migrate).hasMessageContaining("not a Unity Catalog database");
  }

  @Test
  void refusesASchemaNewerThanThisRelease() {
    new SchemaMigrator(db.dataSource(), NEXT_RELEASE).migrate();
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.AHEAD);

    assertThatThrownBy(migrator::migrate).hasMessageContaining("newer than this release");
  }

  @Test
  void refusesADatabaseOfAnotherProduct() {
    assertThatThrownBy(() -> new SchemaMigrator(withProductName("Oracle")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported database Oracle: use H2, PostgreSQL or MySQL");
  }

  @Test
  void runsStartingAtOnceOnAnEmptyDatabaseTakeTurns() throws Exception {
    // The test holds the run lock, so both runs must wait for it before they change anything.
    CountDownLatch waitingInTheDatabase = new CountDownLatch(2);
    DataSource watched =
        withLockCall(
            MigratorTest::isBlockingLock,
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
          runs.add(executor.submit(() -> new SchemaMigrator(watched).migrate()));
        }
        if (isH2()) {
          awaitUntil(() -> held.h2Waiting() == 2);
        } else {
          assertThat(waitingInTheDatabase.await(1, TimeUnit.MINUTES)).isTrue();
        }
        assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.EMPTY);
      }

      int applied = 0;
      for (Future<Integer> run : runs) {
        applied += run.get(2, TimeUnit.MINUTES);
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
    new SchemaMigrator(db.dataSource()).migrate();
    // What a run that failed in step 2 leaves behind on H2 and MySQL; PostgreSQL rolls it back.
    String q = identifierQuote(db.dataSource());
    UpgradeTestUtils.execute(
        db,
        "UPDATE %1$suc_schema_history%1$s SET %1$ssuccess%1$s = false WHERE %1$sversion%1$s = '2'"
            .formatted(q));

    assertThatThrownBy(new SchemaMigrator(db.dataSource())::migrate)
        .hasMessageContaining("Detected failed migration to version 2");
  }

  @ParameterizedTest
  @EnumSource(RefusedLock.class)
  void stopsWhenTheRunLockIsRefused(RefusedLock refused) {
    assumeFalse(isH2(), "H2 has no run lock");
    DataSource refusing =
        switch (refused) {
          case CALL_FAILS -> withLockCall(MigratorTest::isTryLock, sql -> null);
          case NOT_GRANTED ->
              withLockCall(
                  sql -> isTryLock(sql) || isBlockingLock(sql),
                  sql -> "SELECT " + (isTryLock(sql) ? "false" : "NULL"));
        };

    AbstractThrowableAssert<?, ? extends Throwable> thrown =
        assertThatThrownBy(new SchemaMigrator(refusing)::migrate)
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

    assertThatThrownBy(new SchemaMigrator(failing)::migrate)
        .hasMessageContaining("Could not release the schema migration lock");
    // The lock's database session was closed, so a run on other connections takes it at once.
    try (HikariDataSource other = new HikariDataSource()) {
      Properties properties = db.hibernateProperties();
      other.setJdbcUrl(db.jdbcUrl());
      other.setUsername(properties.getProperty("hibernate.connection.username"));
      other.setPassword(properties.getProperty("hibernate.connection.password"));
      assertThat(
              assertTimeoutPreemptively(
                  Duration.ofMinutes(1), () -> new SchemaMigrator(other).migrate()))
          .isZero();
    }
  }

  /** The number of steps in this release: they are numbered 1, 2, 3 and so on. */
  static int releaseSteps(SchemaMigrator migrator) {
    return Integer.parseInt(migrator.latestVersion());
  }

  private static void awaitUntil(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
    while (!condition.getAsBoolean()) {
      assertThat(System.nanoTime()).as("timed out waiting").isLessThan(deadline);
      Thread.sleep(50);
    }
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
   * instead, or fails when that is {@code null}. Flyway sends its own lock calls as prepared
   * statements, so only the run lock's plain statements match.
   */
  private DataSource withLockCall(Predicate<String> matches, UnaryOperator<String> rewrite) {
    DataSource real = db.dataSource();
    return proxy(
        DataSource.class,
        (proxy, method, args) -> {
          Object result = method.invoke(real, args);
          if (!method.getName().equals("getConnection")) {
            return result;
          }
          Connection connection = (Connection) result;
          return proxy(
              Connection.class,
              (connectionProxy, connectionMethod, connectionArgs) -> {
                Object created = connectionMethod.invoke(connection, connectionArgs);
                if (!connectionMethod.getName().equals("createStatement")) {
                  return created;
                }
                Statement statement = (Statement) created;
                return proxy(
                    Statement.class,
                    (statementProxy, statementMethod, statementArgs) -> {
                      if (statementArgs != null
                          && statementArgs.length == 1
                          && statementArgs[0] instanceof String sql
                          && matches.test(sql)) {
                        String rewritten = rewrite.apply(sql);
                        if (rewritten == null) {
                          throw new SQLException("Test: the lock call failed");
                        }
                        statementArgs = new Object[] {rewritten};
                      }
                      return statementMethod.invoke(statement, statementArgs);
                    });
              });
        });
  }

  /** {@code db}'s data source, but its metadata names {@code productName} as the database. */
  private DataSource withProductName(String productName) {
    DataSource real = db.dataSource();
    return proxy(
        DataSource.class,
        (proxy, method, args) -> {
          Object result = method.invoke(real, args);
          if (!method.getName().equals("getConnection")) {
            return result;
          }
          Connection connection = (Connection) result;
          return proxy(
              Connection.class,
              (connectionProxy, connectionMethod, connectionArgs) -> {
                Object metaData = connectionMethod.invoke(connection, connectionArgs);
                if (!connectionMethod.getName().equals("getMetaData")) {
                  return metaData;
                }
                return proxy(
                    DatabaseMetaData.class,
                    (metaProxy, metaMethod, metaArgs) ->
                        metaMethod.getName().equals("getDatabaseProductName")
                            ? productName
                            : metaMethod.invoke(metaData, metaArgs));
              });
        });
  }

  @SuppressWarnings("unchecked")
  private static <T> T proxy(Class<T> type, InvocationHandler handler) {
    return (T)
        Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) -> {
              try {
                return handler.invoke(proxy, method, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
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
