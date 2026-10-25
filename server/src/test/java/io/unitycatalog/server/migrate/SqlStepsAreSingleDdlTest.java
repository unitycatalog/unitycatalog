package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;

/**
 * Every SQL step of every database holds one DDL statement, so a step either applies whole or not
 * at all, and data changes go in Java steps. A step without statements (a version that changes
 * nothing on that database) is allowed. Every check ({@code checks/<db>/C<version>__*.sql}) holds
 * one query and belongs to the step of its version in {@code <db>}.
 */
public class SqlStepsAreSingleDdlTest {

  private static final Path STEPS = Path.of("server/src/main/resources/db/migration");
  private static final Path CHECKS = STEPS.resolve("checks");

  /** Steps that hold more than one statement, with the reason. */
  private static final Map<String, String> EXEMPT =
      Map.of(
          "h2/V1__baseline_0_7.sql", "the baseline: the whole schema at once",
          "postgresql/V1__baseline_0_7.sql", "the baseline: the whole schema at once",
          "mysql/V1__baseline_0_7.sql", "the baseline: the whole schema at once",
          "mysql/V3__case_sensitive_text.sql", "converts every table; safe to repeat");

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
      if (EXEMPT.containsKey(name)) {
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

  @Test
  void eachCheckIsOneQueryOfAStep() throws IOException {
    List<String> problems = new ArrayList<>();
    List<Path> checks = List.of();
    if (Files.isDirectory(CHECKS)) {
      try (Stream<Path> files = Files.walk(CHECKS)) {
        checks =
            files.filter(f -> f.getFileName().toString().matches("C.*\\.sql")).sorted().toList();
      }
    }
    for (Path check : checks) {
      String name = STEPS.relativize(check).toString().replace('\\', '/');
      Path folder = STEPS.resolve(CHECKS.relativize(check.getParent()));
      MigrationVersion version = version(check);
      boolean hasStep;
      try (Stream<Path> files = Files.list(folder)) {
        hasStep =
            files
                .map(f -> f.getFileName().toString())
                .anyMatch(f -> f.matches("V.*__.*\\.sql") && version(Path.of(f)).equals(version));
      } catch (NoSuchFileException e) {
        hasStep = false;
      }
      if (!hasStep) {
        problems.add(name + " has no step V" + version + " in " + STEPS.relativize(folder));
      }
      List<String> statements = statements(Files.readString(check));
      if (statements.size() != 1) {
        problems.add(name + " has " + statements.size() + " statements");
      } else if (!List.of("SELECT", "WITH").contains(firstWord(statements.get(0)))) {
        problems.add(name + " is not a query: " + statements.get(0));
      }
    }
    assertThat(problems)
        .as(
            "Each check in checks/<db>/ holds one SELECT (or WITH ... SELECT) and has a step of"
                + " the same version in <db>/")
        .isEmpty();
  }

  /** The version of a step or check file: "V1_1__name.sql" is 1.1. */
  private static MigrationVersion version(Path file) {
    String name = file.getFileName().toString();
    return MigrationVersion.fromVersion(name.substring(1, name.indexOf("__")).replace('_', '.'));
  }

  private static String firstWord(String statement) {
    return statement.split("\\s+", 2)[0].toUpperCase(Locale.ROOT);
  }

  private static boolean isDdl(String statement) {
    return DDL.contains(firstWord(statement));
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
