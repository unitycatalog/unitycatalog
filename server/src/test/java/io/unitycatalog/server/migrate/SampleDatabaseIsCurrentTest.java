package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The sample H2 database shipped in etc/db is on this build's latest schema version, and its schema
 * is the one a fresh install builds.
 */
public class SampleDatabaseIsCurrentTest {

  private static final Path SAMPLE = Path.of("etc/db/h2db.mv.db");
  private static final Path DUMP = Path.of("server/src/main/resources/schema/h2.txt");

  @Test
  void sampleDatabaseIsOnTheLatestSchemaVersion(@TempDir Path dir) throws IOException {
    JdbcDataSource dataSource = copyOfSample(dir);

    try (SchemaMigrator migrator = new SchemaMigrator(dataSource, DatabaseKind.H2)) {
      SchemaState state = migrator.state();
      String version = migrator.currentVersion();
      assertThat(state)
          .as(
              "etc/db/h2db is at version %s, this build at %s; from the repo root run"
                  + " `build/sbt server/package`, then `bin/start-uc-server` once (it migrates"
                  + " the sample database as it starts; stop it then), and commit"
                  + " etc/db/h2db.mv.db",
              version == null ? "none" : version, migrator.latestVersion())
          .isEqualTo(SchemaState.CURRENT);
    }
  }

  @Test
  void sampleDatabaseHasTheSchemaOfAFreshInstall(@TempDir Path dir) throws IOException {
    List<String> fresh = Files.readString(DUMP).lines().toList();
    List<String> sample = SchemaSnapshot.of(copyOfSample(dir)).entries();

    List<String> diff = SchemaSnapshot.diff(fresh, sample);
    if (!diff.isEmpty()) {
      fail(
          "The schema of etc/db/h2db differs from a fresh install (%s; -: fresh install,"
              + " +: etc/db/h2db); repair etc/db/h2db so it matches a fresh install (see"
              + " docs/server/upgrades.md, section \"The schema differs\") and commit"
              + " etc/db/h2db.mv.db:%n%s",
          DUMP, String.join("\n", diff));
    }
  }

  /** A copy of the sample database, so the tracked file never changes. */
  private static JdbcDataSource copyOfSample(Path dir) throws IOException {
    Files.copy(SAMPLE, dir.resolve("h2db.mv.db"));
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:file:" + dir.resolve("h2db"));
    dataSource.setUser("");
    return dataSource;
  }
}
