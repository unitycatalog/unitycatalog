package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.fail;

import io.unitycatalog.server.testdatabase.SupportedDatabase;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
            "server/src/test/resources/schema",
            SchemaMigrator.DatabaseKind.fromJdbcUrl(db.jdbcUrl()).folder() + ".txt");

    if ("true".equals(System.getenv(UPDATE_ENV))) {
      Files.createDirectories(dump.getParent());
      Files.writeString(dump, actual);
      return;
    }
    String recorded = Files.exists(dump) ? Files.readString(dump) : "";
    if (!recorded.equals(actual)) {
      fail(
          "The migrated schema differs from %s; regenerate with `build/sbt 'set every Test /"
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

  /**
   * A diff of two sorted dumps: lines only in the recorded one with "-", only in the new with "+".
   */
  private static String diff(Path dump, String recorded, String actual) {
    List<String> before = recorded.lines().toList();
    List<String> after = actual.lines().toList();
    StringBuilder out = new StringBuilder("--- " + dump + "\n+++ migrated schema\n");
    int i = 0;
    int j = 0;
    while (i < before.size() || j < after.size()) {
      int cmp =
          i == before.size() ? 1 : j == after.size() ? -1 : before.get(i).compareTo(after.get(j));
      if (cmp == 0) {
        i++;
        j++;
      } else if (cmp < 0) {
        out.append('-').append(before.get(i++)).append('\n');
      } else {
        out.append('+').append(after.get(j++)).append('\n');
      }
    }
    return out.toString();
  }
}
