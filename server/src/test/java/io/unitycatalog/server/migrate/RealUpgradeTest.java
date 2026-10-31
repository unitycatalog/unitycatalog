package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/**
 * Each release of {@link OldServer#releaseChain()}, oldest first, writes its own copy of the {@link
 * DataSet} to one database; the current server then upgrades it to a new install's schema and reads
 * every copy back. Only the current server reads them: reading them on a release in between would
 * only re-check releases that already shipped.
 *
 * <p>It builds and runs old servers' jars, so it runs only with {@code UC_TEST_UPGRADES=true} (the
 * upgrade tests workflow sets it).
 */
public class RealUpgradeTest {

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  @EnabledIfEnvironmentVariable(named = "UC_TEST_UPGRADES", matches = "true")
  void upgradesAndKeepsTheData(@TempDir Path workDir) {
    List<String> chain = OldServer.releaseChain();
    assertThat(chain).as("no release tags found: git fetch --tags").isNotEmpty();
    List<DataSet> copies = new ArrayList<>();
    for (String release : chain) {
      DataSet copy = new DataSet(release);
      OldServer.run(release, db, workDir, copy::write);
      copies.add(copy);
    }

    try (TestServer server = TestServer.start(db, workDir.resolve("storage"))) {
      assertThat(SchemaSnapshot.of(db.dataSource()))
          .isEqualTo(UpgradeTestUtils.freshInstallSnapshot());
      copies.forEach(copy -> copy.assertReadBack(server.apiClient()));
    }
  }

  /** This commit's shaded jar sets up an empty database, as a packaged deployment runs it. */
  @Test
  @EnabledIfEnvironmentVariable(named = "UC_TEST_UPGRADES", matches = "true")
  void packagedServerSetsUpTheSchema(@TempDir Path workDir) {
    DataSet data = new DataSet("HEAD");
    OldServer.run(
        "HEAD",
        db,
        workDir,
        api -> {
          data.write(api);
          data.assertReadBack(api);
        });
    assertThat(SchemaSnapshot.of(db.dataSource()))
        .isEqualTo(UpgradeTestUtils.freshInstallSnapshot());
  }
}
