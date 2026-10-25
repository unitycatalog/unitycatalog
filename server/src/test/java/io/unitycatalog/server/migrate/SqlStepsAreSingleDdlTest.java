package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Every SQL step of every database holds one DDL statement, so a step either applies whole or not
 * at all, and data changes go in Java steps. A step without statements (a version that changes
 * nothing on that database) is allowed. The first step, the 0.7 baseline, builds the whole schema
 * at once; later baselines are Flyway baseline migrations ({@code B<version>__*.sql}, next to the
 * steps, run only on a new database), which this test does not read.
 */
public class SqlStepsAreSingleDdlTest {

  private static final Path STEPS = Path.of("server/src/main/resources/db/migration");

  private static final String BASELINE = "V1__baseline_0_7.sql";

  private static final List<String> DDL = List.of("CREATE", "ALTER", "DROP", "RENAME");

  @Test
  void eachSqlStepIsOneDdlStatement() throws IOException {
    List<String> problems = new ArrayList<>();
    List<Path> steps;
    try (Stream<Path> files = Files.walk(STEPS)) {
      steps = files.filter(f -> f.getFileName().toString().matches("V.*\\.sql")).sorted().toList();
    }
    assertThat(steps).isNotEmpty();
    for (Path step : steps) {
      String name = STEPS.relativize(step).toString().replace('\\', '/');
      if (step.getFileName().toString().equals(BASELINE)) {
        continue;
      }
      List<String> statements = statements(Files.readString(step));
      if (statements.size() > 1) {
        problems.add(name + " has " + statements.size() + " statements");
      } else if (statements.size() == 1 && !isDdl(statements.get(0))) {
        problems.add(name + " is not DDL: " + statements.get(0));
      }
    }
    assertThat(problems)
        .as(
            "Each SQL step holds one DDL statement (CREATE, ALTER, DROP or RENAME); split it into"
                + " one step per statement and put data changes in a Java step")
        .isEmpty();
  }

  private static boolean isDdl(String statement) {
    String first = statement.split("\\s+", 2)[0].toUpperCase(Locale.ROOT);
    return DDL.contains(first);
  }

  /** The file's statements, without comments; quoted text is kept as it is. */
  private static List<String> statements(String sql) {
    List<String> statements = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int i = 0;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (c == '\'') {
        int end = sql.indexOf('\'', i + 1);
        end = end < 0 ? sql.length() : end + 1;
        current.append(sql, i, end);
        i = end;
      } else if (sql.startsWith("--", i)) {
        int end = sql.indexOf('\n', i);
        i = end < 0 ? sql.length() : end;
      } else if (sql.startsWith("/*", i)) {
        int end = sql.indexOf("*/", i + 2);
        i = end < 0 ? sql.length() : end + 2;
        current.append(' ');
      } else if (c == ';') {
        addIfNotBlank(statements, current);
        i++;
      } else {
        current.append(c);
        i++;
      }
    }
    addIfNotBlank(statements, current);
    return statements;
  }

  private static void addIfNotBlank(List<String> statements, StringBuilder current) {
    String statement = current.toString().strip();
    if (!statement.isEmpty()) {
      statements.add(statement);
    }
    current.setLength(0);
  }
}
