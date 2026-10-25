package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The sample H2 database shipped in etc/db is on this build's latest schema version. When it is
 * behind, the test migrates it in place, so committing it is all that is left to do.
 */
public class SampleDatabaseIsCurrentTest {

  private static final Path SAMPLE = Path.of("etc/db/h2db");

  @Test
  void sampleDatabaseIsOnTheLatestSchemaVersion(@TempDir Path dir) throws IOException {
    // Checked on a copy, so the tracked file changes only when it is behind.
    Files.copy(Path.of(SAMPLE + ".mv.db"), dir.resolve("h2db.mv.db"));
    SchemaState state;
    String version;
    String latest;
    try (SchemaMigrator migrator = new SchemaMigrator(h2(dir.resolve("h2db")))) {
      state = migrator.state();
      version = migrator.currentVersion();
      latest = migrator.latestVersion();
    }
    if (state == SchemaState.PENDING) {
      try (SchemaMigrator migrator = new SchemaMigrator(h2(SAMPLE))) {
        migrator.migrate();
      }
    }
    assertThat(state)
        .as(
            "etc/db/h2db.mv.db is %s at version %s, this build at %s; %s",
            state,
            version == null ? "none" : version,
            latest,
            state == SchemaState.PENDING
                ? "the test migrated it, commit etc/db/h2db.mv.db"
                : "restore it with `git checkout HEAD -- etc/db/h2db.mv.db`")
        .isEqualTo(SchemaState.CURRENT);
  }

  private static DataSource h2(Path database) {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:file:" + database.toAbsolutePath());
    dataSource.setUser("");
    return dataSource;
  }
}
