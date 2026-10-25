package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
    execute(db, "CREATE TABLE casbin_rule (id INT)");
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.EMPTY);

    assertThat(migrator.migrate()).isEqualTo(releaseSteps(migrator));
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void adoptsA07Database() {
    create07Schema(db);
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.UNVERSIONED_UC);

    // Version 1 is recorded as the baseline, not run; the steps after it run.
    assertThat(migrator.migrate()).isEqualTo(releaseSteps(migrator) - 1);
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void refusesADatabaseFromBeforeTheBaselineRelease() {
    create07Schema(db);
    execute(db, "DROP TABLE uc_storage_cleanup_tasks");
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());

    assertThatThrownBy(new SchemaMigrator(db.dataSource())::migrate)
        .hasMessageContaining("from a release before " + SchemaMigrator.BASELINE_RELEASE);
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
    execute(db, "CREATE TABLE orders (id INT)");
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.NOT_UC);

    assertThatThrownBy(migrator::migrate).hasMessageContaining("not a Unity Catalog database");
  }

  @Test
  void secondRunAppliesNothing() {
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    migrator.migrate();

    assertThat(migrator.migrate()).isZero();
  }

  @Test
  void refusesASchemaNewerThanThisRelease() {
    new SchemaMigrator(db.dataSource(), NEXT_RELEASE).migrate();
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.state()).isEqualTo(SchemaState.AHEAD);

    assertThatThrownBy(migrator::migrate).hasMessageContaining("newer than this release");
  }

  @Test
  void concurrentRunsApplyEachStepOnce() throws Exception {
    CyclicBarrier start = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    int applied = 0;
    try {
      List<Future<Integer>> runs = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        runs.add(
            executor.submit(
                () -> {
                  start.await();
                  return new SchemaMigrator(db.dataSource()).migrate();
                }));
      }
      for (Future<Integer> run : runs) {
        applied += run.get(2, TimeUnit.MINUTES);
      }
    } finally {
      executor.shutdownNow();
    }

    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(applied).isEqualTo(releaseSteps(migrator));
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  /** The number of steps in this release: they are numbered 1, 2, 3 and so on. */
  static int releaseSteps(SchemaMigrator migrator) {
    return Integer.parseInt(migrator.latestVersion());
  }

  /** Builds a database of the baseline release: its schema without Flyway's history. */
  static void create07Schema(TestDatabase db) {
    String path =
        "db/migration/"
            + SchemaMigrator.DatabaseKind.fromJdbcUrl(db.jdbcUrl()).folder()
            + "/V1__baseline_0_7.sql";
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

  static void execute(TestDatabase db, String sql) {
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new IllegalStateException(sql, e);
    }
  }
}
