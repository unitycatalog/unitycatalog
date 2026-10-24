package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  private static final List<String> SLOW_STEP =
      List.of("classpath:io/unitycatalog/server/migrate/kit/slowstep");

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void setsUpAnEmptyDatabaseOnFirstStart() {
    try (TestServer server = TestServer.start(db)) {
      assertThat(new Migrator(db.dataSource(), db.kind()).state()).isEqualTo(SchemaState.CURRENT);
    }
  }

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
        Migrator migrator = new Migrator(db.dataSource(), db.kind(), SLOW_STEP);
        assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
        int releaseSteps = MigratorTest.releaseSteps(new Migrator(db.dataSource(), db.kind()));
        assertThat(migrator.applied().stream().map(MigrationInfo::getVersion).toList())
            .doesNotHaveDuplicates()
            .hasSize(releaseSteps + 1);
      }
    } finally {
      V1000__Slow_test_step.release();
    }
  }

  @Test
  void refusesA07DatabaseUntilItIsMigrated() {
    MigratorTest.create07Schema(db);
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());

    assertThatThrownBy(() -> TestServer.start(db))
        .hasMessageContaining("no schema version")
        .hasMessageContaining("bin/uc-migrate migrate");
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);

    new Migrator(db.dataSource(), db.kind()).migrate();
    try (TestServer server = TestServer.start(db)) {
      assertThat(new Migrator(db.dataSource(), db.kind()).state()).isEqualTo(SchemaState.CURRENT);
    }
  }

  @Test
  void refusesASchemaNewerThanThisRelease() {
    new Migrator(db.dataSource(), db.kind(), List.of("classpath:db/test-steps/ahead")).migrate();

    assertThatThrownBy(() -> TestServer.start(db)).hasMessageContaining("this release ends at");
  }
}
