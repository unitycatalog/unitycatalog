package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.migrate.kit.DataSet;
import io.unitycatalog.server.migrate.kit.OldImage;
import io.unitycatalog.server.migrate.kit.OldServer;
import io.unitycatalog.server.migrate.kit.TestServer;
import io.unitycatalog.server.migrate.kit.UpgradePath;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A database every release of the {@link UpgradePath} wrote its own copy of the {@link DataSet} to,
 * in turn, upgrades on the current server's start to the schema of a new install and keeps every
 * copy. Only the current server reads the copies back: reading them back on a release in between
 * would only re-check releases that already shipped.
 *
 * <p>Tagged {@code upgrades}: it runs old servers' jars, so it stays out of the default test run
 * ({@code build/sbt testUpgrades} runs it).
 */
public class RealUpgradeTest {

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  @Tag("upgrades")
  void upgradesAndKeepsTheData() {
    List<OldImage> chain = UpgradePath.releaseChain();
    assertThat(chain).as("no release tags found: git fetch --tags").isNotEmpty();
    List<DataSet> copies = new ArrayList<>();
    for (OldImage image : chain) {
      DataSet copy = new DataSet(image.toString());
      OldServer.run(image, db, copy::write);
      copies.add(copy);
    }

    try (TestServer server = TestServer.start(db)) {
      assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(freshInstallSnapshot());
      copies.forEach(copy -> copy.assertReadBack(server.apiClient()));
    }
  }

  /** The schema the steps build on a new empty database. */
  static SchemaSnapshot freshInstallSnapshot() {
    try (TestDatabase fresh = TestDatabases.newEmptyDatabase();
        SchemaMigrator migrator = new SchemaMigrator(fresh.dataSource(), fresh.kind())) {
      migrator.migrate();
      return SchemaSnapshot.of(fresh.dataSource());
    }
  }
}
