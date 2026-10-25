package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SchemaGateTest {

  /** A test-only step 1000: the next release's step for a current database. */
  private static final List<String> AHEAD = List.of("classpath:db/test-steps/ahead");

  @TempDatabase(empty = true)
  TestDatabase db;

  @TempDir Path storageRoot;

  @Test
  void setsUpAnEmptyDatabaseOnFirstStart() {
    try (TestServer server = TestServer.start(db, storageRoot)) {
      assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.CURRENT);
    }
  }

  @Test
  void upgradesA07Database() {
    MigratorTest.create07Schema(db);

    try (TestServer server = TestServer.start(db, storageRoot)) {
      assertThat(SchemaSnapshot.of(db.dataSource()))
          .isEqualTo(RealUpgradeTest.freshInstallSnapshot());
    }
  }

  @Test
  void refusesASchemaNewerThanThisRelease() {
    new SchemaMigrator(db.dataSource(), AHEAD).migrate();

    assertThatThrownBy(() -> TestServer.start(db, storageRoot))
        .hasMessageContaining("this release ends at");
  }
}
