package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.fail;

import io.unitycatalog.server.testdatabase.SupportedDatabase;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * The schema the steps build matches the dump recorded for the selected database, so a change to
 * the steps shows up in the pull request as a change to the dump. The dump covers tables, columns,
 * keys, indexes, foreign keys and CHECK constraints; MySQL collations are not in it. So does the
 * schema of the baseline step alone, which the server compares a database it adopts with.
 */
public class SchemaDumpTest {

  private static final String UPDATE_ENV = "UC_UPDATE_SCHEMA_DUMPS";

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void migratedSchemaMatchesTheRecordedDump() throws IOException {
    try (SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind())) {
      migrator.migrate();
    }
    assertMatchesTheDump(Path.of("server/src/main/resources/schema"));
  }

  @Test
  void baselineSchemaMatchesTheRecordedDump() throws IOException {
    MigratorTest.create07Schema(db);
    assertMatchesTheDump(Path.of("server/src/main/resources/schema/baseline"));
  }

  /** The database's schema matches the dump of its kind in {@code folder}, or rewrites it. */
  private void assertMatchesTheDump(Path folder) throws IOException {
    String actual = SchemaSnapshot.of(db.dataSource()).render();
    Path dump = folder.resolve(db.kind().folder() + ".txt");

    if ("true".equals(System.getenv(UPDATE_ENV))) {
      Files.createDirectories(dump.getParent());
      Files.writeString(dump, actual);
      return;
    }
    String recorded = Files.exists(dump) ? Files.readString(dump) : "";
    if (!recorded.equals(actual)) {
      fail(
          "The schema differs from %s; regenerate with `build/sbt 'set every Test /"
              + " envVars := Map(\"%s\" -> \"%s\", \"%s\" -> \"true\")' 'server/testOnly %s'`"
              + " and commit the file:%n%s",
          dump,
          SupportedDatabase.ENV,
          SupportedDatabase.selected(),
          UPDATE_ENV,
          getClass().getName(),
          diff(dump, recorded, actual));
    }
  }

  /** A diff of two dumps: lines only in the recorded one with "-", only in the new with "+". */
  private static String diff(Path dump, String recorded, String actual) {
    return "--- %s%n+++ migrated schema%n%s"
        .formatted(
            dump,
            SchemaSnapshot.diff(recorded.lines().toList(), actual.lines().toList()).stream()
                .map(line -> line + "\n")
                .collect(Collectors.joining()));
  }
}
