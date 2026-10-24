package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.migrate.kit.AcrossUpgradePaths;
import io.unitycatalog.server.migrate.kit.DataSet;
import io.unitycatalog.server.migrate.kit.OldServer;
import io.unitycatalog.server.migrate.kit.SchemaSnapshot;
import io.unitycatalog.server.migrate.kit.TestServer;
import io.unitycatalog.server.migrate.kit.UpgradePath;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;

/** Databases real servers wrote, on every {@link UpgradePath}, upgrade and keep their data. */
public class RealUpgradeTest {

  @TempDatabase(empty = true)
  TestDatabase db;

  @AcrossUpgradePaths
  void upgradesAndKeepsTheData(UpgradePath path) {
    OldServer.run(path.images.get(0), db, DataSet::write);
    path.images.stream().skip(1).forEach(image -> OldServer.run(image, db, api -> {}));
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());

    try (Migrator migrator = new Migrator(db.dataSource(), db.kind())) {
      if (migrator.state() != SchemaState.CURRENT) {
        assertThatThrownBy(() -> TestServer.start(db))
            .hasMessageContaining("bin/uc-migrate migrate");
        assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);
      }
      assertThat(migrator.migrate().to()).isEqualTo(migrator.latestVersion());
    }
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(freshInstallSnapshot());
    try (TestServer server = TestServer.start(db)) {
      DataSet.assertReadBack(server.apiClient());
    }
  }

  /** The schema the steps build on a new empty database. */
  private static SchemaSnapshot freshInstallSnapshot() {
    try (TestDatabase fresh = TestDatabases.newEmptyDatabase();
        Migrator migrator = new Migrator(fresh.dataSource(), fresh.kind())) {
      migrator.migrate();
      return SchemaSnapshot.of(fresh.dataSource());
    }
  }
}
