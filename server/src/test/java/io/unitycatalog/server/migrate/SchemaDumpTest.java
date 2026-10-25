package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * The schema the steps build matches the dump recorded for the selected database, so a change to
 * the steps shows up in the pull request as a change to the dump. The dump covers tables, columns,
 * keys, indexes, foreign keys and CHECK constraints; MySQL collations are not in it.
 */
public class SchemaDumpTest {

  private static final String UPDATE_ENV = "UC_UPDATE_SCHEMA_DUMPS";

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void migratedSchemaMatchesTheRecordedDump() throws IOException {
    try (SchemaMigrator migrator = new SchemaMigrator(db.dataSource())) {
      migrator.migrate();
    }
    String actual = SchemaSnapshot.of(db.dataSource()).render();
    Path dump =
        Path.of(
            "server/src/main/resources/schema",
            SchemaMigrator.DatabaseKind.fromJdbcUrl(db.jdbcUrl()).folder() + ".txt");

    if ("true".equals(System.getenv(UPDATE_ENV))) {
      Files.createDirectories(dump.getParent());
      Files.writeString(dump, actual);
      return;
    }
    String recorded = Files.exists(dump) ? Files.readString(dump) : "";
    List<String> before = recorded.lines().toList();
    List<String> after = actual.lines().toList();
    // Only the lines that differ: "-" only in the recorded dump, "+" only in the migrated schema.
    List<String> diff =
        Stream.concat(
                before.stream().filter(line -> !after.contains(line)).map(line -> "-" + line),
                after.stream().filter(line -> !before.contains(line)).map(line -> "+" + line))
            .toList();
    assertThat(diff)
        .as(
            "The migrated schema differs from %s; run `build/sbt updateSchemaDumps` (needs Docker)"
                + " and commit the files",
            dump)
        .isEmpty();
  }
}
