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
 * Every SQL step of every database holds one statement, and the statement is one of the changes
 * {@link #ALLOWED} lists: an allow-list of changes that existing rows can't make fail and that
 * servers of the previous release keep working with. The test reads the statement, not the column
 * it changes, so review checks that a type change widens the column and that a MySQL {@code MODIFY}
 * does not add NOT NULL. A step either applies whole or not at all. A step without statements (a
 * version that changes nothing on that database) is allowed. The first step, the 0.7 baseline,
 * builds the whole schema at once; later baselines are Flyway baseline migrations ({@code
 * B<version>__*.sql}, next to the steps, run only on a new database), which this test does not
 * read.
 */
public class MigrationStepRulesTest {

  private static final Path STEPS = Path.of("server/src/main/resources/db/migration");

  private static final String BASELINE = "V1__baseline_0_7.sql";

  private static final String NAME = "\\w+";

  /**
   * An index name, but not CONCURRENTLY, which PostgreSQL can't run inside a step's transaction.
   */
  private static final String INDEX_NAME = "(?!concurrently\\b)\\w+";

  /** A column name, but not a keyword that starts a constraint or an index. */
  private static final String COLUMN =
      "(?!(?:constraint|primary|unique|foreign|check|index|key)\\b)\\w+";

  /** A type, with an optional size: {@code text}, {@code varchar(64)}, {@code decimal(10, 2)}. */
  private static final String TYPE = "\\w+(?:\\s*\\(\\s*\\d+(?:\\s*,\\s*\\d+)?\\s*\\))?";

  private static final String LITERAL = "(?:'[^']*'|-?\\d+(?:\\.\\d+)?|true|false)";

  /** A new column is nullable, or NOT NULL with a default that fills the existing rows. */
  private static final String NEW_COLUMN_NULLABILITY =
      "(?:\\s+null|\\s+default\\s+"
          + LITERAL
          + "(?:\\s+not\\s+null)?|\\s+not\\s+null\\s+default\\s+"
          + LITERAL
          + ")?";

  /**
   * What a step may hold outside its {@code '...'} strings and {@code --} comments. The allowed
   * changes need no more, and other quotes and comments ({@code "}, a backtick, MySQL's {@code #},
   * H2's {@code //}, block comments, which PostgreSQL and H2 nest, and {@code $$} strings) could
   * hide a {@code ;} from this test.
   */
  private static final Pattern PLAIN = Pattern.compile("[\\w\\s(),.=<>-]");

  /**
   * The changes a step may make, each matched against the whole statement, ignoring case. Not
   * listed, so refused: data changes, unique indexes and key, foreign key, CHECK or NOT NULL rules
   * on an existing table (existing rows can break them; a MySQL {@code MODIFY} restates NOT NULL,
   * so review checks it), and drops and renames (servers of the previous release still use the old
   * schema during an upgrade).
   */
  private static final List<Pattern> ALLOWED =
      Stream.of(
              // A new table, with any columns and rules: it has no rows yet.
              "create\\s+table\\s+" + NAME + "\\s*\\(.*",
              // An index that is not unique.
              "create\\s+index\\s+"
                  + INDEX_NAME
                  + "\\s+on\\s+"
                  + NAME
                  + "\\s*\\(\\s*"
                  + NAME
                  + "(?:\\s*,\\s*"
                  + NAME
                  + ")*\\s*\\)",
              // A new column, without a key or CHECK rule.
              "alter\\s+table\\s+"
                  + NAME
                  + "\\s+add\\s+(?:column\\s+)?"
                  + COLUMN
                  + "\\s+"
                  + TYPE
                  + NEW_COLUMN_NULLABILITY,
              // A column's new type (H2, PostgreSQL).
              "alter\\s+table\\s+"
                  + NAME
                  + "\\s+alter\\s+column\\s+"
                  + NAME
                  + "\\s+(?:set\\s+data\\s+type\\s+|type\\s+)?"
                  + TYPE,
              // A column's new type (MySQL, which restates the nullability).
              "alter\\s+table\\s+"
                  + NAME
                  + "\\s+modify\\s+(?:column\\s+)?"
                  + NAME
                  + "\\s+"
                  + TYPE
                  + "(?:\\s+(?:not\\s+)?null)?",
              // A column that may now be NULL (H2, PostgreSQL).
              "alter\\s+table\\s+"
                  + NAME
                  + "\\s+alter\\s+column\\s+"
                  + NAME
                  + "\\s+drop\\s+not\\s+null")
          .map(regex -> Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL))
          .toList();

  @Test
  void eachSqlStepIsOneAllowedChange() throws IOException {
    List<Path> steps;
    try (Stream<Path> files = Files.walk(STEPS)) {
      steps = files.filter(f -> f.getFileName().toString().matches("V.*\\.sql")).sorted().toList();
    }
    assertThat(steps).isNotEmpty();
    List<String> problems = new ArrayList<>();
    for (Path step : steps) {
      String name = STEPS.relativize(step).toString().replace('\\', '/');
      if (step.getFileName().toString().equals(BASELINE)) {
        continue;
      }
      List<String> statements;
      try {
        statements = statements(Files.readString(step));
      } catch (IllegalArgumentException e) {
        problems.add(name + " " + e.getMessage());
        continue;
      }
      if (statements.size() > 1) {
        problems.add(name + " has " + statements.size() + " statements");
      } else if (statements.size() == 1
          && ALLOWED.stream().noneMatch(p -> p.matcher(statements.get(0)).matches())) {
        problems.add(name + " is not an allowed change: " + statements.get(0));
      }
    }
    assertThat(problems)
        .as(
            "Each SQL step holds one statement that creates a table, creates an index that is not"
                + " unique, adds a column (NOT NULL only with a DEFAULT, without a key or CHECK),"
                + " widens a column's type or drops its NOT NULL. Other changes are not supported"
                + " yet; see ALLOWED in this test")
        .isEmpty();
  }

  /**
   * The file's statements, without {@code --} comments; {@code '...'} strings are kept as they are.
   *
   * @throws IllegalArgumentException for text this splitter may not read the way the database does:
   *     a character that {@link #PLAIN} doesn't list, a backslash in a string (MySQL's escape), a
   *     {@code --} that MySQL doesn't read as a comment, or a carriage return without a line feed
   */
  private static List<String> statements(String sql) {
    if (sql.replace("\r\n", "\n").indexOf('\r') >= 0) {
      throw new IllegalArgumentException("has a carriage return without a line feed after it");
    }
    List<String> statements = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int i = 0;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (c == '\'') {
        int end = sql.indexOf('\'', i + 1);
        end = end < 0 ? sql.length() : end + 1;
        if (sql.substring(i, end).contains("\\")) {
          throw new IllegalArgumentException(
              "has a backslash in a string, which MySQL reads as an escape: write a quote as ''");
        }
        current.append(sql, i, end);
        i = end;
      } else if (sql.startsWith("--", i)) {
        // MySQL reads -- as a comment only when a space or a line end follows it.
        if (i + 2 < sql.length() && !Character.isWhitespace(sql.charAt(i + 2))) {
          throw new IllegalArgumentException("has -- without a space after it: write '-- '");
        }
        int end = sql.indexOf('\n', i);
        i = end < 0 ? sql.length() : end;
      } else if (c == ';') {
        addIfNotBlank(statements, current);
        i++;
      } else if (PLAIN.matcher(String.valueOf(c)).matches()) {
        current.append(c);
        i++;
      } else {
        throw new IllegalArgumentException(
            "has "
                + c
                + " outside a string: write names unquoted, strings in '...' and comments after"
                + " --");
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
