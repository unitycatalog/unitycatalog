package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
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
import org.junit.jupiter.api.Test;

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
          SchemaMigrator.RunLock.acquire(
              db.dataSource(), SchemaMigrator.DatabaseKind.fromJdbcUrl(db.jdbcUrl()))) {
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

  @Test
  void stopsWhenTheLockCallFails() {
    assumeFalse(isH2(), "H2 has no run lock");
    DataSource failing = withLockCall(MigratorTest::isTryLock, sql -> null);

    assertThatThrownBy(new SchemaMigrator(failing)::migrate)
        .hasMessageContaining("Could not take the schema migration lock");
    assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.EMPTY);
  }

  @Test
  void stopsWhenTheLockIsNotGranted() {
    assumeFalse(isH2(), "H2 has no run lock");
    // Another run holds the lock, and the blocking call returns NULL (on MySQL: interrupted).
    DataSource refusing =
        withLockCall(
            sql -> isTryLock(sql) || isBlockingLock(sql),
            sql -> "SELECT " + (isTryLock(sql) ? "false" : "NULL"));

    assertThatThrownBy(new SchemaMigrator(refusing)::migrate)
        .hasMessageContaining("Could not take the schema migration lock")
        .hasMessageContaining("returned NULL");
    assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.EMPTY);
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
        (method, args) -> {
          Object result = method.invoke(real, args);
          if (!method.getName().equals("getConnection")) {
            return result;
          }
          Connection connection = (Connection) result;
          return proxy(
              Connection.class,
              (connectionMethod, connectionArgs) -> {
                Object created = connectionMethod.invoke(connection, connectionArgs);
                if (!connectionMethod.getName().equals("createStatement")) {
                  return created;
                }
                Statement statement = (Statement) created;
                return proxy(
                    Statement.class,
                    (statementMethod, statementArgs) -> {
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

  @SuppressWarnings("unchecked")
  private static <T> T proxy(Class<T> type, InvocationHandlerBody body) {
    return (T)
        Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) -> {
              try {
                return body.invoke(method, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  private interface InvocationHandlerBody {
    Object invoke(Method method, Object[] args) throws Throwable;
  }

  /** The quote for identifiers: Flyway's history table has quoted lower-case column names. */
  private static String identifierQuote(DataSource dataSource) {
    try (Connection connection = dataSource.getConnection()) {
      return connection.getMetaData().getIdentifierQuoteString();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }
}
