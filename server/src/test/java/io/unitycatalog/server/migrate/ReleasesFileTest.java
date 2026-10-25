package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;

/**
 * {@code releases.properties} lists every release from the baseline release, in ascending order,
 * each with the last step it ships: a step of every database, or a version up to the baseline's.
 */
public class ReleasesFileTest {

  private static final Path MIGRATION = Path.of("server/src/main/resources/db/migration");
  private static final Path JAVA_STEPS =
      Path.of("server/src/main/java/io/unitycatalog/server/migrate/steps");

  @Test
  void listsEachReleaseInOrderWithAStepOfEveryDatabase() throws IOException {
    List<String> releases = new ArrayList<>();
    List<String> problems = new ArrayList<>();
    MigrationVersion lastRelease = null;
    MigrationVersion lastStep = null;
    for (String line : Files.readAllLines(MIGRATION.resolve("releases.properties"))) {
      if (line.isBlank() || line.startsWith("#")) {
        continue;
      }
      String[] parts = line.split("=", 2);
      MigrationVersion release = MigrationVersion.fromVersion(parts[0].trim());
      MigrationVersion step = MigrationVersion.fromVersion(parts[1].trim());
      releases.add(parts[0].trim());
      if (lastRelease != null && release.compareTo(lastRelease) <= 0) {
        problems.add(line + ": not above the release before it");
      }
      if (lastStep != null && step.compareTo(lastStep) < 0) {
        problems.add(line + ": its last step is below the release before it's");
      }
      if (step.compareTo(MigrationVersion.fromVersion("1")) > 0) {
        for (DatabaseKind kind : DatabaseKind.values()) {
          if (!hasStep(MIGRATION.resolve(kind.folder()), step) && !hasStep(JAVA_STEPS, step)) {
            problems.add(line + ": no step " + step + " for " + kind.folder());
          }
        }
      }
      lastRelease = release;
      lastStep = step;
    }
    assertThat(releases).first().isEqualTo(SchemaMigrator.BASELINE_RELEASE);
    assertThat(problems).isEmpty();
  }

  private static boolean hasStep(Path folder, MigrationVersion version) throws IOException {
    try (Stream<Path> files = Files.list(folder)) {
      return files
          .map(f -> f.getFileName().toString())
          .filter(f -> f.matches("V[0-9_.]+__.*\\.(sql|java)"))
          .anyMatch(
              f ->
                  MigrationVersion.fromVersion(f.substring(1, f.indexOf("__")).replace('_', '.'))
                      .equals(version));
    }
  }
}
