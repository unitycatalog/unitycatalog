package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The sample H2 database shipped in etc/db is on this build's latest schema version. */
public class SampleDatabaseIsCurrentTest {

  private static final Path SAMPLE = Path.of("etc/db/h2db.mv.db");

  @Test
  void sampleDatabaseIsOnTheLatestSchemaVersion(@TempDir Path dir) throws IOException {
    // A copy, so the tracked file never changes.
    Files.copy(SAMPLE, dir.resolve("h2db.mv.db"));
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:file:" + dir.resolve("h2db"));
    dataSource.setUser("");

    try (SchemaMigrator migrator = new SchemaMigrator(dataSource)) {
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
}
