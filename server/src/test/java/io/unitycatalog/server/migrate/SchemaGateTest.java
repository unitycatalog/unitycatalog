package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.migrate.kit.SchemaSnapshot;
import io.unitycatalog.server.migrate.kit.TestServer;
import io.unitycatalog.server.migrate.kit.slowstep.V1000__Slow_test_step;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.flywaydb.core.api.MigrationInfo;
import org.junit.jupiter.api.Test;

public class SchemaGateTest {

  /** A test-only step 1000: the next release's step for a current database. */
  private static final List<String> AHEAD = List.of("classpath:db/test-steps/ahead");

  private static final List<String> SLOW_STEP =
      List.of("classpath:io/unitycatalog/server/migrate/kit/slowstep");

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void setsUpAnEmptyDatabaseOnFirstStart() {
    try (TestServer server = TestServer.start(db)) {
      assertThat(new SchemaMigrator(db.dataSource(), db.kind()).state())
          .isEqualTo(SchemaState.CURRENT);
    }
  }

  // A smoke test of the server path; MigratorTest.concurrentRunsApplyEachStepOnce proves the run
  // lock.
  @Test
  void twoServersStartingAtOnceSetUpTheSchemaOnce() throws Exception {
    V1000__Slow_test_step.reset();
    CompletableFuture<TestServer> first =
        CompletableFuture.supplyAsync(() -> TestServer.start(db, SLOW_STEP));
    CompletableFuture<TestServer> second = null;
    try {
      assertThat(V1000__Slow_test_step.awaitStarted()).isTrue();
      second = CompletableFuture.supplyAsync(() -> TestServer.start(db, SLOW_STEP));
      Thread.sleep(1000); // let the second server reach the run lock
      V1000__Slow_test_step.release();

      try (TestServer s1 = first.get();
          TestServer s2 = second.get()) {
        SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind(), SLOW_STEP);
        assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
        int releaseSteps =
            MigratorTest.releaseSteps(new SchemaMigrator(db.dataSource(), db.kind()));
        assertThat(migrator.applied().stream().map(MigrationInfo::getVersion).toList())
            .doesNotHaveDuplicates()
            .hasSize(releaseSteps + 1);
      }
    } finally {
      V1000__Slow_test_step.release();
    }
  }

  @Test
  void upgradesA07Database() {
    MigratorTest.create07Schema(db);

    try (TestServer server = TestServer.start(db)) {
      assertThat(SchemaSnapshot.of(db.dataSource()))
          .isEqualTo(RealUpgradeTest.freshInstallSnapshot());
    }
  }

  @Test
  void appliesPendingSteps() {
    new SchemaMigrator(db.dataSource(), db.kind()).migrate();

    try (TestServer server = TestServer.start(db, AHEAD)) {
      assertThat(new SchemaMigrator(db.dataSource(), db.kind(), AHEAD).state())
          .isEqualTo(SchemaState.CURRENT);
    }
  }

  @Test
  void refusesASchemaNewerThanThisRelease() {
    new SchemaMigrator(db.dataSource(), db.kind(), AHEAD).migrate();

    assertThatThrownBy(() -> TestServer.start(db)).hasMessageContaining("this release ends at");
  }

  @Test
  void refusesADatabaseOfAnotherApplication() {
    MigratorTest.execute(db, "CREATE TABLE orders (id INT)");

    assertThatThrownBy(() -> TestServer.start(db))
        .hasMessageContaining("not a Unity Catalog database");
  }

  @Test
  void refusesADatabaseWithAFailedStep() {
    new SchemaMigrator(db.dataSource(), db.kind()).migrate();
    MigratorTest.recordFailedStep(db);

    assertThatThrownBy(() -> TestServer.start(db))
        .hasMessageContaining("A migration step failed in an earlier run: version 1000 (broken)");
  }
}
