package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Each schema step has an effect file that lists its change ({@link #eachStepHasItsEffectFile}), so
 * that {@code --upgrade-schema} can compare a database's schema with the schema of each version.
 */
public class MigrationStepRulesTest {

  private static final Path EFFECTS = Path.of("server/src/main/resources/schema-effects");

  @TempDatabase(empty = true)
  TestDatabase db;

  /**
   * Each step's effect file lists what the step changes in the schema the snapshot reads, as the
   * step does it on a new database after the steps before it. With {@code
   * UC_UPDATE_SCHEMA_EFFECTS=true} ({@code build/sbt updateSchemaDumps}) the test writes the files
   * instead.
   */
  @Test
  void eachStepHasItsEffectFile() throws IOException {
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    DatabaseKind kind = DatabaseKind.of(db.dataSource());
    Path folder = EFFECTS.resolve(kind.folder());
    boolean update = "true".equals(System.getenv("UC_UPDATE_SCHEMA_EFFECTS"));
    List<String> problems = new ArrayList<>();
    SchemaSnapshot before = SchemaSnapshot.NONE;
    for (String step : migrator.steps()) {
      UpgradeTestUtils.migrateTo(db, step);
      SchemaSnapshot after = SchemaSnapshot.of(db.dataSource());
      String expected = String.join("\n", before.changesTo(after)) + "\n";
      Path file = folder.resolve("V" + step + ".txt");
      if (update) {
        Files.createDirectories(folder);
        Files.writeString(file, expected);
      } else if (!Files.exists(file) || !Files.readString(file).equals(expected)) {
        problems.add(
            file
                + " is missing or out of date (run build/sbt updateSchemaDumps); expected:\n"
                + expected);
      }
      before = after;
    }
    assertThat(problems).isEmpty();
  }
}
