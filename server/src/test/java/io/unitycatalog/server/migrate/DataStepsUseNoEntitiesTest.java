package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Data steps use plain JDBC: entities and repositories change with each release, and a step must
 * keep working on the schema of its own release.
 */
public class DataStepsUseNoEntitiesTest {

  private static final Path STEPS =
      Path.of("server/src/main/java/io/unitycatalog/server/migrate/steps");

  private static final Pattern PERSIST_IMPORT =
      Pattern.compile("(?m)^\\s*import\\s+(static\\s+)?io\\.unitycatalog\\.server\\.persist\\b");

  @Test
  void dataStepsImportNothingFromPersist() throws IOException {
    List<Path> sources;
    try (Stream<Path> files = Files.walk(STEPS)) {
      sources = files.filter(f -> f.toString().endsWith(".java")).sorted().toList();
    }
    assertThat(sources).isNotEmpty();
    List<String> offenders = new ArrayList<>();
    for (Path source : sources) {
      if (PERSIST_IMPORT.matcher(Files.readString(source)).find()) {
        offenders.add(STEPS.relativize(source).toString());
      }
    }
    assertThat(offenders)
        .as(
            "Data steps import nothing from io.unitycatalog.server.persist (entities, DAOs,"
                + " repositories); use plain JDBC on the step's connection")
        .isEmpty();
  }
}
