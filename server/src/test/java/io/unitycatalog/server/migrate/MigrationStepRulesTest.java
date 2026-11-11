package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The rules every schema step keeps, so that {@code --upgrade-schema} can tell from the schema
 * whether a step ran, and so that rows already in the database can't make a step fail:
 *
 * <ul>
 *   <li>A step is one SQL statement, as Flyway sends it to each database, or none when the step
 *       changes nothing on that database. The first step, the 0.7.0 baseline, may create any table
 *       and index.
 *   <li>The statement is one of the changes {@link #refusal} allows, read with JSqlParser from what
 *       Flyway sent: create a table, create a plain index (not unique, fulltext or spatial), add a
 *       column (NOT NULL only with a DEFAULT, without a key or CHECK rule), change a column's type,
 *       or drop its NOT NULL. A type change must widen the column, and no change makes a column NOT
 *       NULL ({@link #typeChangesWiden}).
 *   <li>Each step has an effect file that lists its change ({@link #eachStepHasItsEffectFile}).
 *   <li>Every step exists for all three databases, as a versioned SQL file.
 * </ul>
 *
 * <p>It runs with {@code UC_TEST_MIGRATION_CHECKS=true}, on each database.
 */
@EnabledIfEnvironmentVariable(named = "UC_TEST_MIGRATION_CHECKS", matches = "true")
public class MigrationStepRulesTest {

  private static final Path STEPS = Path.of("server/src/main/resources/db/migration");

  private static final Path EFFECTS = Path.of("server/src/main/resources/schema-effects");

  private static final Pattern STEP_FILE = Pattern.compile("V(\\d+)__\\w+\\.sql");

  /** What a new column may say besides its type: nullable, or NOT NULL with a default. */
  private static final Pattern NEW_COLUMN =
      Pattern.compile(
          "(NULL|DEFAULT \\S+|DEFAULT (?!NULL )\\S+ NOT NULL|NOT NULL DEFAULT (?!NULL$)\\S+)?");

  /**
   * The type changes that widen a column, by database: from the type before to the types after. The
   * same type widens when its size grows.
   */
  private static final Map<DatabaseKind, Map<String, Set<String>>> WIDER =
      Map.of(
          DatabaseKind.H2,
          Map.of(
              "character varying", Set.of("character large object"),
              "smallint", Set.of("integer", "bigint"),
              "integer", Set.of("bigint")),
          DatabaseKind.POSTGRESQL,
          Map.of(
              "varchar", Set.of("text"),
              "int2", Set.of("int4", "int8"),
              "int4", Set.of("int8")),
          DatabaseKind.MYSQL,
          Map.of(
              "varchar", Set.of("text", "mediumtext", "longtext"),
              "text", Set.of("mediumtext", "longtext"),
              "mediumtext", Set.of("longtext"),
              "smallint", Set.of("int", "bigint"),
              "int", Set.of("bigint")));

  private static final Pattern COLUMN_LINE =
      Pattern.compile(
          "([-+]) column (\\S+) type=(.+) size=(\\d+) (?:scale=(\\d+) )?.*nullable=(YES|NO) .*");

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
      // A step that shows no change cannot be told from one that did not run.
      if (before.equals(after) && !parse(Files.readString(stepFile(kind, step))).isEmpty()) {
        problems.add("step " + step + " changes nothing the schema snapshot shows");
      }
      before = after;
    }
    assertThat(problems).isEmpty();
  }

  /**
   * Each step after the first is one allowed statement, or none, as Flyway sends it to this
   * database.
   */
  @Test
  void eachStepIsOneAllowedStatement() throws IOException {
    DatabaseKind kind = DatabaseKind.of(db.dataSource());
    List<String> problems = new ArrayList<>();
    Map<String, List<String>> sentByStep = UpgradeTestUtils.sentStatements(db);
    String first = sentByStep.keySet().iterator().next();
    for (Map.Entry<String, List<String>> entry : sentByStep.entrySet()) {
      String step = entry.getKey();
      Path file = stepFile(kind, step);
      List<String> sent = entry.getValue();
      if (!step.equals(first) && sent.size() > 1) {
        problems.add(file + " sends " + sent.size() + " statements; a step holds one: " + sent);
      }
      for (String sql : sent) {
        String refusal = refusalOfSent(sql, kind, step.equals(first));
        if (refusal != null) {
          problems.add(file + ": " + refusal + ": " + sql);
        }
      }
    }
    assertThat(problems)
        .as(
            "Each step after the first holds one statement that creates a table, creates an index"
                + " that is not unique, adds a column (NOT NULL only with a DEFAULT, without a key"
                + " or CHECK rule), changes a column's type or drops its NOT NULL. Other changes"
                + " are not supported yet; see refusal() in this test")
        .isEmpty();
  }

  /**
   * What a step after the first may send: a change that rows already in the database can't make
   * fail. A new column may be NOT NULL only with a default that is not NULL, in either order.
   */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "POSTGRESQL|ALTER TABLE t ADD c INT DEFAULT NULL NOT NULL|is not an allowed change",
        "POSTGRESQL|ALTER TABLE t ADD c INT NOT NULL DEFAULT NULL|is not an allowed change",
        "POSTGRESQL|ALTER TABLE t ADD c INT NOT NULL|is not an allowed change",
        "POSTGRESQL|ALTER TABLE t ADD c INT NOT NULL DEFAULT 0|",
        "POSTGRESQL|ALTER TABLE t ADD c INT DEFAULT 0 NOT NULL|",
        "POSTGRESQL|ALTER TABLE t ADD c INT DEFAULT NULL|",
        "POSTGRESQL|ALTER TABLE t ADD c INT|",
        "POSTGRESQL|CREATE INDEX i ON t (c)|",
        "POSTGRESQL|CREATE INDEX CONCURRENTLY i ON t (c)|creates an index CONCURRENTLY",
        "POSTGRESQL|CREATE UNIQUE INDEX i ON t (c)|creates a unique index",
        "MYSQL|CREATE FULLTEXT INDEX i ON t (c)|creates a fulltext index",
        "POSTGRESQL|ALTER TABLE t ALTER COLUMN c TYPE text|",
        "POSTGRESQL|ALTER TABLE t ALTER COLUMN c TYPE int USING c::int|is not an allowed change",
        "POSTGRESQL|ALTER TABLE t ALTER COLUMN c DROP NOT NULL|",
        "POSTGRESQL|ALTER TABLE t ALTER COLUMN c SET NOT NULL|is not an allowed change",
        "POSTGRESQL|ALTER TABLE t ADD UNIQUE (c)|is not an allowed change",
        "POSTGRESQL|ALTER TABLE t ADD CONSTRAINT k CHECK (c > 0)|is not an allowed change",
        "POSTGRESQL|ALTER TABLE t DROP COLUMN c|is not an allowed change",
        "H2|ALTER TABLE t ALTER COLUMN c CLOB|",
        "MYSQL|ALTER TABLE t ALTER COLUMN c TYPE text|is not an allowed change",
        "MYSQL|ALTER TABLE t MODIFY c mediumtext NOT NULL|",
        "MYSQL|ALTER TABLE t MODIFY c INT PRIMARY KEY|is not an allowed change"
      })
  void laterStepsOnlyMakeChangesRowsCannotFail(DatabaseKind kind, String sql, String refusal) {
    assertThat(refusalOfSent(sql, kind, false)).isEqualTo(refusal);
  }

  /** Every step is a versioned SQL file, and exists for all three databases. */
  @Test
  void everyStepExistsForEveryDatabase() throws IOException {
    Map<String, Set<String>> versions = new TreeMap<>();
    List<String> problems = new ArrayList<>();
    for (DatabaseKind kind : DatabaseKind.values()) {
      try (Stream<Path> files = Files.list(STEPS.resolve(kind.folder()))) {
        for (Path file : files.toList()) {
          Matcher name = STEP_FILE.matcher(file.getFileName().toString());
          if (name.matches()) {
            versions.computeIfAbsent(name.group(1), v -> new TreeSet<>()).add(kind.folder());
          } else {
            problems.add(file + " is not a step: name steps V<version>__<description>.sql");
          }
        }
      }
    }
    versions.forEach(
        (version, folders) -> {
          if (folders.size() != DatabaseKind.values().length) {
            problems.add(
                "step " + version + " exists only in " + folders + ": add it, empty if need be");
          }
        });
    assertThat(problems).isEmpty();
  }

  /**
   * A step that changes a column (its effect file has a {@code -} and a {@code +} line for it)
   * widens its type and does not make it NOT NULL: rows already in the database would not fit.
   */
  @Test
  void typeChangesWiden() throws IOException {
    List<String> problems = new ArrayList<>();
    for (DatabaseKind kind : DatabaseKind.values()) {
      try (Stream<Path> files = Files.list(EFFECTS.resolve(kind.folder()))) {
        for (Path file : files.toList()) {
          for (String problem : narrowedColumns(kind, Files.readAllLines(file))) {
            problems.add(file + ": " + problem);
          }
        }
      }
    }
    assertThat(problems).isEmpty();
  }

  /** Column changes that rows already in the database could fail, and ones they can't. */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      quoteCharacter = '"',
      value = {
        "POSTGRESQL|type=varchar size=255 nullable=YES"
            + "|type=varchar size=255 nullable=NO|becomes NOT NULL",
        "POSTGRESQL|type=varchar size=255 nullable=YES"
            + "|type=varchar size=100 nullable=YES|not wider",
        "POSTGRESQL|type=numeric size=10 scale=2 nullable=YES"
            + "|type=numeric size=10 scale=1 nullable=YES|not wider",
        "POSTGRESQL|type=int8 size=19 nullable=YES|type=int4 size=10 nullable=YES|not wider",
        "POSTGRESQL|type=varchar size=255 nullable=YES|type=text size=2147483647 nullable=YES|",
        "POSTGRESQL|type=varchar size=255 nullable=NO|type=varchar size=255 nullable=YES|",
        "MYSQL|type=enum size=8 nullable=NO default=null values=enum('FUNCTION','TABLE')"
            + "|type=enum size=8 nullable=NO default=null values=enum('FUNCTION')"
            + "|drops the values [TABLE]",
        "MYSQL|type=enum size=8 nullable=NO default=null values=enum('FUNCTION','TABLE')"
            + "|type=enum size=8 nullable=NO default=null values=enum('FUNCTION','TABLE','VIEW')|",
        "POSTGRESQL|type=varchar size=255 nullable=|type=varchar size=100 nullable=YES|can't read"
      })
  void columnChangesWiden(DatabaseKind kind, String before, String after, String problem) {
    List<String> found =
        narrowedColumns(
            kind, List.of("- column t.c " + before + " ", "+ column t.c " + after + " "));
    if (problem == null) {
      assertThat(found).isEmpty();
    } else {
      assertThat(found).singleElement().asString().contains(problem);
    }
  }

  /**
   * What the column changes in an effect file's {@code lines} do that rows already in the database
   * could fail, one entry per change; a column line it can't read is one too.
   */
  private static List<String> narrowedColumns(DatabaseKind kind, List<String> lines) {
    List<String> problems = new ArrayList<>();
    Map<String, Matcher> removed = new HashMap<>();
    for (String line : lines) {
      if (!line.matches("[-+] column .*")) {
        continue;
      }
      Matcher column = COLUMN_LINE.matcher(line);
      if (!column.matches()) {
        problems.add("can't read the column line " + line);
      } else if (column.group(1).equals("-")) {
        removed.put(column.group(2), column);
      } else if (removed.containsKey(column.group(2))) {
        Matcher before = removed.get(column.group(2));
        String problem = narrowing(kind, before, column);
        if (problem == null) {
          problem = droppedValues(before.group(), line);
        }
        if (problem != null) {
          problems.add(column.group(2) + " " + problem);
        }
      }
    }
    return problems;
  }

  /** MySQL's enum and set members, in a column line's {@code values=}. */
  private static final Pattern VALUES = Pattern.compile(" values=(?:enum|set)\\('(.*)'\\)");

  /** The enum or set members that column line {@code after} drops from {@code before}, or null. */
  private static String droppedValues(String before, String after) {
    List<String> kept = members(after);
    List<String> dropped = members(before).stream().filter(v -> !kept.contains(v)).toList();
    return dropped.isEmpty() ? null : "drops the values " + dropped;
  }

  /** The enum or set members in a column line, or none. */
  private static List<String> members(String line) {
    Matcher values = VALUES.matcher(line);
    return values.find() ? List.of(values.group(1).split("','")) : List.of();
  }

  private static long scale(Matcher column) {
    return column.group(5) == null ? 0 : Long.parseLong(column.group(5));
  }

  private static String sizeAndScale(Matcher column) {
    return column.group(4) + (column.group(5) == null ? "" : "," + column.group(5));
  }

  /** Why the change from column line {@code before} to {@code after} narrows it, or null. */
  private static String narrowing(DatabaseKind kind, Matcher before, Matcher after) {
    if (before.group(6).equals("YES") && after.group(6).equals("NO")) {
      return "becomes NOT NULL";
    }
    String from = before.group(3);
    String to = after.group(3);
    boolean wider = from.equals(to) || WIDER.get(kind).getOrDefault(from, Set.of()).contains(to);
    // A number keeps its digits before the decimal point (size minus scale) and its scale.
    long scaleBefore = scale(before);
    long scaleAfter = scale(after);
    boolean fewerDigits =
        Long.parseLong(after.group(4)) - scaleAfter < Long.parseLong(before.group(4)) - scaleBefore;
    if (!wider || fewerDigits || scaleAfter < scaleBefore) {
      return "changes from "
          + from
          + "("
          + sizeAndScale(before)
          + ") to "
          + to
          + "("
          + sizeAndScale(after)
          + "), which is not wider";
    }
    return null;
  }

  /** Why a step may not send {@code sql}, or {@code null} when it may. */
  private static String refusalOfSent(String sql, DatabaseKind kind, boolean firstStep) {
    try {
      List<Statement> parsed = parse(sql);
      return parsed.size() != 1 ? "is not one statement" : refusal(parsed.get(0), kind, firstStep);
    } catch (IllegalArgumentException e) {
      return e.getMessage();
    }
  }

  /**
   * Why a step may not hold {@code statement}, or {@code null} when it may. The first step may
   * create any table and index (other than from a query, or CONCURRENTLY); a later step only makes
   * a change that rows already in the database can't make fail.
   */
  private static String refusal(Statement statement, DatabaseKind kind, boolean firstStep) {
    if (statement instanceof CreateTable table) {
      return table.getSelect() == null ? null : "creates a table from a query";
    }
    if (statement instanceof CreateIndex index) {
      if (index.toString().toUpperCase(Locale.ROOT).contains(" CONCURRENTLY ")) {
        // On PostgreSQL a failed CONCURRENTLY leaves an index that is not valid.
        return "creates an index CONCURRENTLY";
      }
      String type = index.getIndex().getType();
      if (firstStep || type == null) {
        return null;
      }
      return "creates a " + type.toLowerCase(Locale.ROOT) + " index";
    }
    if (firstStep) {
      return "the first step only creates tables and indexes";
    }
    if (!(statement instanceof Alter alter) || alter.getAlterExpressions().size() != 1) {
      return "is not an allowed change";
    }
    AlterExpression change = alter.getAlterExpressions().get(0);
    boolean allowed =
        switch (change.getOperation()) {
          // ALTER TABLE t ADD c <type> [NULL | DEFAULT x | NOT NULL DEFAULT x]
          case ADD -> NEW_COLUMN.matcher(columnSpecs(change)).matches();
          // PostgreSQL and H2: ALTER TABLE t ALTER COLUMN c [SET DATA] TYPE <type>, or
          // ALTER TABLE t ALTER COLUMN c DROP NOT NULL
          case ALTER -> kind != DatabaseKind.MYSQL && (changesType(change) || dropsNotNull(change));
          // MySQL: ALTER TABLE t MODIFY c <type> [NULL | NOT NULL]
          case MODIFY -> kind == DatabaseKind.MYSQL && columnSpecs(change).matches("(NOT )?NULL|");
          default -> false;
        };
    return allowed ? null : "is not an allowed change";
  }

  /**
   * The words after the type of the one column that {@code change} adds, changes or modifies, in
   * upper case; {@code "?"} (which no shape allows) when it names no column or several.
   */
  private static String columnSpecs(AlterExpression change) {
    List<AlterExpression.ColumnDataType> columns = change.getColDataTypeList();
    if (columns == null || columns.size() != 1 || change.getIndex() != null) {
      return "?";
    }
    return String.join(" ", columns.get(0).getColumnSpecs()).toUpperCase(Locale.ROOT);
  }

  private static boolean changesType(AlterExpression change) {
    return columnSpecs(change).isEmpty()
        && change.getColDataTypeList().get(0).getUsingExpression() == null;
  }

  private static boolean dropsNotNull(AlterExpression change) {
    return change.getColDataTypeList() == null
        && change.getColumnDropNotNullList() != null
        && change.getColumnDropNotNullList().size() == 1;
  }

  /**
   * The statements JSqlParser reads in {@code sql}.
   *
   * @throws IllegalArgumentException when it can't read it: what it can't read, it can't check
   */
  private static List<Statement> parse(String sql) {
    try {
      return CCJSqlParserUtil.parseStatements(sql).stream().toList();
    } catch (JSQLParserException e) {
      throw new IllegalArgumentException(
          "JSqlParser can't read it: " + e.getMessage().lines().findFirst().orElse(""), e);
    }
  }

  /** The SQL file of step {@code version} for {@code kind}. */
  private static Path stepFile(DatabaseKind kind, String version) throws IOException {
    try (Stream<Path> files = Files.list(STEPS.resolve(kind.folder()))) {
      return files
          .filter(f -> f.getFileName().toString().startsWith("V" + version + "__"))
          .findFirst()
          .orElseThrow();
    }
  }
}
