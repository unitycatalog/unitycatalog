package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.migrate.kit.SchemaSnapshot;
import io.unitycatalog.server.migrate.kit.TestServer;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SchemaGateTest {

  /** A test-only step 1000: the next release's step for a current database. */
  private static final List<String> AHEAD = List.of("classpath:db/test-steps/ahead");

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void setsUpAnEmptyDatabaseOnFirstStart() {
    try (TestServer server = TestServer.start(db)) {
      assertThat(new SchemaMigrator(db.dataSource(), db.kind()).state())
          .isEqualTo(SchemaState.CURRENT);
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
  void refusesASchemaNewerThanThisRelease() {
    new SchemaMigrator(db.dataSource(), db.kind(), AHEAD).migrate();

    assertThatThrownBy(() -> TestServer.start(db)).hasMessageContaining("this release ends at");
  }
}
