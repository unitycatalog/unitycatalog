package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import net.sf.jsqlparser.statement.create.table.Index;
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
 *       Flyway sent: create a table, create a plain index (not unique, fulltext or spatial) whose
 *       PostgreSQL index entry, INCLUDE columns too, fits 2,704 bytes, add a column (NOT NULL only
 *       with a literal DEFAULT, without a key or CHECK rule), change a column's type (each index on
 *       the column still fitting an index entry), or drop its NOT NULL.
 *   <li>A type change widens the column, keeps its default, collation and enum or set members, and
 *       no change makes an existing nullable column NOT NULL ({@link #typeChangesWiden}). Adding a
 *       Java enum constant is supported on MySQL and H2, where the type holds the members; on
 *       PostgreSQL it means replacing a CHECK constraint, which no allowed change does yet.
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

  /**
   * A default that is a literal: a number, a quoted string, or TRUE or FALSE. An expression such as
   * {@code (NULL)}, {@code NULL::integer} or {@code abs(NULL)} can be NULL or fail on each row.
   */
  private static final String LITERAL = "(-?\\d+(\\.\\d+)?|'[^' ]*'|TRUE|FALSE)";

  /** What a MySQL MODIFY may say besides the type: nullability, and the default and collation. */
  private static final Pattern MODIFY_SPECS =
      Pattern.compile("((NOT NULL|NULL|DEFAULT (NULL|" + LITERAL + ")|COLLATE \\S+)( (?=\\S)|$))*");

  /** What a new column may say besides its type: nullable, or NOT NULL with a literal default. */
  private static final Pattern NEW_COLUMN =
      Pattern.compile(
          "(NULL|DEFAULT (NULL|"
              + LITERAL
              + ")|DEFAULT "
              + LITERAL
              + " NOT NULL|NOT NULL DEFAULT "
              + LITERAL
              + ")?");

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

  /**
   * {@code build/sbt updateSchemaDumps} sets this to write the effect files. The checks that read
   * them skip that run, since its test classpath predates the files it writes; the next test run
   * checks them.
   */
  private static final boolean WRITES_EFFECTS =
      "true".equals(System.getenv("UC_UPDATE_SCHEMA_EFFECTS"));

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
    List<String> problems = new ArrayList<>();
    SchemaSnapshot before = SchemaSnapshot.NONE;
    for (String step : migrator.steps()) {
      UpgradeTestUtils.migrateTo(db, step);
      SchemaSnapshot after = SchemaSnapshot.of(db.dataSource());
      String expected = String.join("\n", before.changesTo(after)) + "\n";
      Path file = folder.resolve("V" + step + ".txt");
      if (WRITES_EFFECTS) {
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
    assumeFalse(WRITES_EFFECTS, "checks the effect files on the next test run");
    DatabaseKind kind = DatabaseKind.of(db.dataSource());
    List<String> problems = new ArrayList<>();
    Map<String, List<String>> sentByStep = UpgradeTestUtils.sentStatements(db);
    String first = sentByStep.keySet().iterator().next();
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    String previous = null;
    for (Map.Entry<String, List<String>> entry : sentByStep.entrySet()) {
      String step = entry.getKey();
      SchemaSnapshot before = previous == null ? SchemaSnapshot.NONE : migrator.expected(previous);
      previous = step;
      Path file = stepFile(kind, step);
      List<String> sent = entry.getValue();
      if (!step.equals(first) && sent.size() > 1) {
        problems.add(file + " sends " + sent.size() + " statements; a step holds one: " + sent);
      }
      for (String sql : sent) {
        String refusal = refusalOfSent(sql, kind, step.equals(first), before);
        if (refusal != null) {
          problems.add(file + ": " + refusal + ": " + sql);
        }
      }
      if (!step.equals(first)) {
        for (String problem : regrownIndexes(kind, before, migrator.expected(step))) {
          problems.add(file + ": " + problem);
        }
      }
    }
    assertThat(problems)
        .as(
            "Each step after the first holds one statement that creates a table, creates a plain"
                + " index on columns short enough for an index entry, adds a column (NOT NULL only"
                + " with a literal DEFAULT, without a key or CHECK rule), changes a column's type"
                + " (each index on it still short enough) or drops its NOT NULL. Other changes are"
                + " not supported yet; see refusal() in this test")
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
        // A default must be a literal: an expression can be NULL, or fail, on each row.
        "POSTGRESQL|ALTER TABLE t ADD c INT DEFAULT (NULL) NOT NULL|is not an allowed change",
        "POSTGRESQL|ALTER TABLE t ADD c INT NOT NULL DEFAULT NULL::int|is not an allowed change",
        "POSTGRESQL|ALTER TABLE t ADD c INT NOT NULL DEFAULT abs(NULL)|is not an allowed change",
        "MYSQL|ALTER TABLE t ADD c INT NOT NULL DEFAULT (abs(NULL))|is not an allowed change",
        "H2|ALTER TABLE t ADD c INT NOT NULL DEFAULT coalesce(NULL)|is not an allowed change",
        "MYSQL|ALTER TABLE t MODIFY c varchar(300) NULL DEFAULT (NULL)|is not an allowed change",
        "POSTGRESQL|ALTER TABLE t ADD c VARCHAR(10) NOT NULL DEFAULT 'x'|",
        "POSTGRESQL|ALTER TABLE t ADD c BOOLEAN NOT NULL DEFAULT FALSE|",
        "POSTGRESQL|ALTER TABLE t ADD c NUMERIC(5,2) NOT NULL DEFAULT -1.5|",
        "POSTGRESQL|ALTER TABLE t ADD c INT|",
        "POSTGRESQL|CREATE INDEX i ON t (c)|",
        // PostgreSQL fails an index entry over 2,704 bytes, so a long text column is refused.
        "POSTGRESQL|CREATE INDEX i ON t (long)|has an index entry of up to 262152 bytes, over the"
            + " 2704 bytes PostgreSQL allows",
        "POSTGRESQL|CREATE INDEX i ON t (c, body)|has an index entry of up to 8589935624 bytes,"
            + " over the 2704 bytes PostgreSQL allows",
        // The limit is for the whole key: two 600-character columns can hold 4,800 bytes.
        "POSTGRESQL|CREATE INDEX i ON t (a, b)|has an index entry of up to 4824 bytes, over the"
            + " 2704 bytes PostgreSQL allows",
        "POSTGRESQL|CREATE INDEX i ON t (a, id)|",
        // An entry adds an 8-byte header and a 4-byte length per text value, rounded up to 8.
        // An INCLUDE column is stored in the entry as well.
        "POSTGRESQL|CREATE INDEX i ON t (c) INCLUDE (long)|has an index entry of up to 263176"
            + " bytes, over the 2704 bytes PostgreSQL allows",
        // PostgreSQL's bit(n) holds n bits; MySQL's bit is at most 8 bytes.
        "POSTGRESQL|CREATE INDEX i ON t (bits)|has an index entry of up to 2768 bytes, over the"
            + " 2704 bytes PostgreSQL allows",
        "POSTGRESQL|CREATE INDEX i ON t (flag)|",
        "MYSQL|CREATE INDEX i ON t (flag)|",
        // Each column is padded to 8 bytes before the next one, here before the int8.
        "POSTGRESQL|CREATE INDEX i ON t (a40, big, c630)|has an index entry of up to 2712 bytes,"
            + " over the 2704 bytes PostgreSQL allows",
        "H2|CREATE INDEX i ON t (bin)|",
        "H2|CREATE INDEX i ON t (binbig)|has an index entry of up to 100016 bytes, over the 2704"
            + " bytes PostgreSQL allows",
        "POSTGRESQL|CREATE INDEX i ON t (c676)|has an index entry of up to 2720 bytes, over the"
            + " 2704 bytes PostgreSQL allows",
        "POSTGRESQL|CREATE INDEX i ON t (c673)|",
        "POSTGRESQL|CREATE INDEX i ON t (json)|indexes t.json (jsonb), which this test can't size",
        "POSTGRESQL|CREATE INDEX i ON t (data)|indexes t.data (bytea), which this test can't size",
        "MYSQL|CREATE INDEX i ON t (long(100))|",
        "POSTGRESQL|CREATE INDEX i ON t (long DESC)|has an index entry of up to 262152 bytes, over"
            + " the 2704 bytes PostgreSQL allows",
        "POSTGRESQL|CREATE INDEX i ON t (lower(c))|indexes an expression, which this test can't"
            + " size",
        "POSTGRESQL|CREATE INDEX i ON t (missing)|indexes t.missing, which the schema before the"
            + " step doesn't have",
        "POSTGRESQL|CREATE INDEX CONCURRENTLY i ON t (c)|creates an index CONCURRENTLY",
        "POSTGRESQL|CREATE INDEX i ON t (c) WHERE CAST(c AS integer) > 0|creates a partial index,"
            + " whose predicate this test can't check",
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
        "MYSQL|ALTER TABLE t MODIFY c varchar(20) NOT NULL DEFAULT 'x'|",
        "MYSQL|ALTER TABLE t MODIFY c varchar(20) COLLATE utf8mb4_bin NULL|",
        "MYSQL|ALTER TABLE t MODIFY c INT PRIMARY KEY|is not an allowed change"
      })
  void laterStepsOnlyMakeChangesRowsCannotFail(DatabaseKind kind, String sql, String refusal) {
    SchemaSnapshot before =
        new SchemaSnapshot(
            List.of(
                "column t.c type=varchar size=255 nullable=YES default=null",
                "column t.long type=varchar size=65535 nullable=YES default=null",
                "column t.body type=text size=2147483647 nullable=YES default=null",
                "column t.a type=varchar size=600 nullable=YES default=null",
                "column t.b type=varchar size=600 nullable=YES default=null",
                "column t.id type=uuid size=2147483647 nullable=NO default=null",
                "column t.data type=bytea size=2147483647 nullable=YES default=null",
                "column t.json type=jsonb size=2147483647 nullable=YES default=null",
                "column t.c676 type=varchar size=676 nullable=YES default=null",
                "column t.c673 type=varchar size=673 nullable=YES default=null",
                "column t.bits type=bit size=22000 nullable=YES default=null",
                "column t.flag type=bit size=1 nullable=YES default=null",
                "column t.a40 type=varchar size=40 nullable=YES default=null",
                "column t.big type=int8 size=19 nullable=YES default=null",
                "column t.c630 type=varchar size=630 nullable=YES default=null",
                "column t.bin type=binary varying size=100 nullable=YES default=null",
                "column t.binbig type=binary varying size=100000 nullable=YES default=null"));
    assertThat(refusalOfSent(sql, kind, false, before)).isEqualTo(refusal);
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
    assumeFalse(WRITES_EFFECTS, "checks the effect files on the next test run");
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
        // PostgreSQL reports a numeric without precision, which holds any number, as size 0.
        "POSTGRESQL|type=numeric size=0 scale=0 nullable=YES"
            + "|type=numeric size=10 scale=2 nullable=YES|not wider",
        "POSTGRESQL|type=numeric size=10 scale=2 nullable=YES|type=numeric size=0 scale=0"
            + " nullable=YES|",
        // pgjdbc reports numeric(4,-2), which rounds to hundreds, with scale 2046 (-2 in 11 bits).
        "POSTGRESQL|type=numeric size=4 scale=2046 nullable=YES|type=numeric size=4 scale=0"
            + " nullable=YES|numeric(4,-2) to numeric(4,0), which is not wider",
        "POSTGRESQL|type=numeric size=4 scale=2046 nullable=YES|type=numeric size=6 scale=0"
            + " nullable=YES|",
        "POSTGRESQL|type=numeric size=4 scale=2046 nullable=YES|type=numeric size=6 scale=2046"
            + " nullable=YES|",
        // H2 names an enum's members in its type: keeping them all and adding one widens it.
        "H2|type=enum('A') size=1 nullable=YES|type=enum('A', 'B') size=1 nullable=YES|",
        "H2|type=enum('A', 'B') size=1 nullable=YES|type=enum('A') size=1 nullable=YES|not wider",
        // A MySQL MODIFY that does not restate the default or collation drops it.
        "MYSQL|type=varchar size=10 nullable=NO default=x"
            + "|type=varchar size=20 nullable=NO default=null|changes its default from x to null",
        "MYSQL|type=varchar size=10 nullable=YES default=null collation=utf8mb4_bin"
            + "|type=varchar size=20 nullable=YES default=null|changes its collation",
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
   * A type change keeps each index on the column short enough for an index entry, sized with the
   * new type; an index whose columns keep their types isn't checked again.
   */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "type=varchar size=100|type=varchar size=100|type=int4 size=10|type=int8 size=19|",
        // Later rows can hold the longer values.
        "type=varchar size=100|type=varchar size=5000|type=int4 size=10|type=int4 size=10"
            + "|up to 20024 bytes",
        // PostgreSQL rebuilds the index for int4 to int8, and rows already there can outgrow it.
        "type=varchar size=673|type=varchar size=673|type=int4 size=10|type=int8 size=19"
            + "|up to 2712 bytes",
        "type=varchar size=673|type=varchar size=673|type=int4 size=10|type=int4 size=10|",
        // Dropping NOT NULL doesn't grow an entry, even in an index already over the estimate.
        "type=varchar size=2048 nullable=NO|type=varchar size=2048 nullable=YES|type=int4 size=10"
            + "|type=int4 size=10|",
        "type=varchar size=2048|type=varchar size=4096|type=int4 size=10|type=int4 size=10"
            + "|up to 16408 bytes"
      })
  void typeChangesKeepIndexEntriesShort(
      String textBefore,
      String textAfter,
      String numberBefore,
      String numberAfter,
      String problem) {
    List<String> index =
        List.of("index t.k unique=false column=c#1", "index t.k unique=false column=i#2");
    SchemaSnapshot before = snapshot(textBefore, numberBefore, index);
    SchemaSnapshot after = snapshot(textAfter, numberAfter, index);
    List<String> found = regrownIndexes(DatabaseKind.POSTGRESQL, before, after);
    if (problem == null) {
      assertThat(found).isEmpty();
    } else {
      assertThat(found).singleElement().asString().contains(problem);
    }
  }

  private static SchemaSnapshot snapshot(String text, String number, List<String> index) {
    List<String> entries = new ArrayList<>(index);
    entries.add(
        "column t.c "
            + text
            + (text.contains(" nullable=") ? "" : " nullable=YES")
            + " default=null");
    entries.add("column t.i " + number + " nullable=YES default=null");
    return new SchemaSnapshot(entries);
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
        if (problem == null) {
          problem = changedAttribute(before.group(), line);
        }
        if (problem != null) {
          problems.add(column.group(2) + " " + problem);
        }
      }
    }
    return problems;
  }

  /** A column line's default and collation; MySQL's MODIFY drops what it does not restate. */
  private static final Pattern DEFAULT =
      Pattern.compile(" default=(.*?)(?: values=| collation=| ?$)");

  private static final Pattern COLLATION = Pattern.compile(" collation=(\\S+)");

  /** How column line {@code after} changes or drops the default or collation of {@code before}. */
  private static String changedAttribute(String before, String after) {
    for (Pattern attribute : List.of(DEFAULT, COLLATION)) {
      String from = attribute(attribute, before);
      String to = attribute(attribute, after);
      if (!from.equals(to)) {
        String name = attribute == DEFAULT ? "default" : "collation";
        return "changes its " + name + " from " + from + " to " + to;
      }
    }
    return null;
  }

  private static String attribute(Pattern attribute, String line) {
    Matcher value = attribute.matcher(line);
    return value.find() ? value.group(1) : "none";
  }

  /** MySQL's enum and set members, in a column line's {@code values=}. */
  private static final Pattern VALUES = Pattern.compile(" values=(?:enum|set)\\('(.*)'\\)");

  /** The enum or set members that column line {@code after} drops from {@code before}, or null. */
  private static String droppedValues(String before, String after) {
    List<String> kept = members(after);
    List<String> dropped = members(before).stream().filter(v -> !kept.contains(v)).toList();
    return dropped.isEmpty() ? null : "drops the values " + dropped;
  }

  /**
   * Whether H2 enum type {@code to} keeps every member of {@code from} (H2 names them in the type).
   */
  private static boolean keepsEnumMembers(String from, String to) {
    return from.startsWith("enum(")
        && to.startsWith("enum(")
        && members(" values=" + to).containsAll(members(" values=" + from));
  }

  /** The enum or set members in a column line, or none. */
  private static List<String> members(String line) {
    Matcher values = VALUES.matcher(line);
    // MySQL separates members with ',', H2 with ', '.
    return values.find() ? List.of(values.group(1).split("',\\s*'")) : List.of();
  }

  /** PostgreSQL reports a {@code numeric} without precision, which holds any number, as size 0. */
  private static boolean isUnbounded(DatabaseKind kind, Matcher column) {
    return kind == DatabaseKind.POSTGRESQL
        && column.group(3).equals("numeric")
        && column.group(4).equals("0");
  }

  /** The digits a number column keeps before the decimal point (size minus scale). */
  private static long integerDigits(DatabaseKind kind, Matcher column) {
    return isUnbounded(kind, column)
        ? Long.MAX_VALUE
        : Long.parseLong(column.group(4)) - scale(kind, column);
  }

  private static long scale(DatabaseKind kind, Matcher column) {
    if (isUnbounded(kind, column)) {
      return Long.MAX_VALUE;
    }
    if (column.group(5) == null) {
      return 0;
    }
    long scale = Long.parseLong(column.group(5));
    // pgjdbc reports a negative scale in 11 bits, so -2 reads as 2046 (PostgreSQL allows -1000 to
    // 1000).
    return kind == DatabaseKind.POSTGRESQL && scale >= 1024 ? scale - 2048 : scale;
  }

  private static String shape(DatabaseKind kind, Matcher column) {
    if (isUnbounded(kind, column)) {
      return column.group(3);
    }
    return column.group(3)
        + "("
        + column.group(4)
        + (column.group(5) == null ? "" : "," + scale(kind, column))
        + ")";
  }

  /** Why the change from column line {@code before} to {@code after} narrows it, or null. */
  private static String narrowing(DatabaseKind kind, Matcher before, Matcher after) {
    if (before.group(6).equals("YES") && after.group(6).equals("NO")) {
      return "becomes NOT NULL";
    }
    String from = before.group(3);
    String to = after.group(3);
    boolean wider =
        from.equals(to)
            || WIDER.get(kind).getOrDefault(from, Set.of()).contains(to)
            || keepsEnumMembers(from, to);
    // A number keeps its digits before the decimal point and its scale.
    boolean fewerDigits = integerDigits(kind, after) < integerDigits(kind, before);
    if (!wider || fewerDigits || scale(kind, after) < scale(kind, before)) {
      return "changes from "
          + shape(kind, before)
          + " to "
          + shape(kind, after)
          + ", which is not wider";
    }
    return null;
  }

  /** Why a step may not send {@code sql}, or {@code null} when it may. */
  private static String refusalOfSent(
      String sql, DatabaseKind kind, boolean firstStep, SchemaSnapshot before) {
    try {
      List<Statement> parsed = parse(sql);
      return parsed.size() != 1
          ? "is not one statement"
          : refusal(parsed.get(0), kind, firstStep, before);
    } catch (IllegalArgumentException e) {
      return e.getMessage();
    }
  }

  /**
   * Why a step may not hold {@code statement}, or {@code null} when it may. The first step may
   * create any table and index (other than from a query, or CONCURRENTLY); a later step only makes
   * a change that rows already in the database can't make fail.
   */
  private static String refusal(
      Statement statement, DatabaseKind kind, boolean firstStep, SchemaSnapshot before) {
    if (statement instanceof CreateTable table) {
      return table.getSelect() == null ? null : "creates a table from a query";
    }
    if (statement instanceof CreateIndex index) {
      if (index.toString().toUpperCase(Locale.ROOT).contains(" CONCURRENTLY ")) {
        // On PostgreSQL a failed CONCURRENTLY leaves an index that is not valid.
        return "creates an index CONCURRENTLY";
      }
      String type = index.getIndex().getType();
      if (firstStep) {
        return null;
      }
      if (type != null) {
        return "creates a " + type.toLowerCase(Locale.ROOT) + " index";
      }
      if (index.getWhere() != null) {
        // PostgreSQL evaluates the predicate on every row, which can fail on rows already there.
        return "creates a partial index, whose predicate this test can't check";
      }
      return indexedColumnProblem(kind, index, before);
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
          // MySQL: ALTER TABLE t MODIFY c <type> [NULL | NOT NULL] [DEFAULT x] [COLLATE x], which
          // must restate the column's default and collation (typeChangesWiden checks they stay)
          case MODIFY ->
              kind == DatabaseKind.MYSQL && MODIFY_SPECS.matcher(columnSpecs(change)).matches();
          default -> false;
        };
    return allowed ? null : "is not an allowed change";
  }

  /**
   * PostgreSQL fails an index entry over 2,704 bytes (a third of a page); MySQL's 3,072-byte key
   * limit already fails the step on an empty database. An entry that fits 2,704 fits both.
   */
  private static final long MAX_INDEX_ENTRY_BYTES = 2704;

  /**
   * The most bytes a column of {@code bytes} takes in a PostgreSQL index entry: a 4-byte length
   * before a {@code variable} value, rounded up to 8 to cover the padding before the next column.
   */
  private static long entryBytes(long bytes, boolean variable) {
    return (bytes + (variable ? 4 : 0) + 7) / 8 * 8;
  }

  /** The bytes each fixed-width type, as the snapshot names it, takes in an index entry. */
  private static final Map<String, Long> FIXED_WIDTH_BYTES =
      Map.ofEntries(
          Map.entry("boolean", 1L),
          Map.entry("bool", 1L),
          Map.entry("bit", 8L),
          Map.entry("tinyint", 1L),
          Map.entry("smallint", 2L),
          Map.entry("int2", 2L),
          Map.entry("integer", 4L),
          Map.entry("int", 4L),
          Map.entry("int4", 4L),
          Map.entry("oid", 4L),
          Map.entry("bigint", 8L),
          Map.entry("int8", 8L),
          Map.entry("timestamp", 8L),
          Map.entry("datetime", 8L),
          Map.entry("uuid", 16L));

  /**
   * The most bytes a value of {@code type} with {@code size} takes in an index entry: 4 per
   * character for text, the size for binary; null for a type this test can't bound.
   */
  private static Long maxBytes(DatabaseKind kind, String type, long size) {
    if (isPostgresqlBits(kind, type)) {
      return 4 + (size + 7) / 8; // a 4-byte bit count, then the bits
    }
    if (FIXED_WIDTH_BYTES.containsKey(type)) {
      return FIXED_WIDTH_BYTES.get(type);
    }
    if (type.startsWith("enum")) {
      return 2L;
    }
    if (type.contains("large object") || type.equals("bytea")) {
      return null;
    }
    if (type.contains("char") || type.contains("text")) {
      return 4 * size;
    }
    if (type.contains("binary")) {
      return size;
    }
    return null;
  }

  /** PostgreSQL's {@code bit(n)} and {@code varbit} hold n bits; the snapshot reports n as size. */
  private static boolean isPostgresqlBits(DatabaseKind kind, String type) {
    return kind == DatabaseKind.POSTGRESQL && (type.equals("bit") || type.equals("varbit"));
  }

  /** Whether a value of {@code type} has a length before it in an index entry. */
  private static boolean isVariableWidth(DatabaseKind kind, String type) {
    return isPostgresqlBits(kind, type)
        || !(FIXED_WIDTH_BYTES.containsKey(type) || type.startsWith("enum"));
  }

  /**
   * Why rows already in the database could fail {@code index}, against the schema {@code before}
   * the step: its key can be longer than an index entry (a MySQL prefix length counts as that many
   * characters), or it holds a type or expression this test can't bound; or null.
   */
  private static String indexedColumnProblem(
      DatabaseKind kind, CreateIndex index, SchemaSnapshot before) {
    String table = unquoted(index.getTable().getName());
    List<Index.ColumnParams> elements = new ArrayList<>(index.getIndex().getColumns());
    // PostgreSQL stores INCLUDE columns in the entry too.
    for (String name :
        index.getIncludeColumns() == null ? List.<String>of() : index.getIncludeColumns()) {
      elements.add(new Index.ColumnParams(name));
    }
    long indexEntryBytes = 8; // the entry's header
    for (Index.ColumnParams element : elements) {
      // JSqlParser keeps a MySQL prefix length as "(100)", a sort order as "DESC", and a
      // function's arguments (an expression) as "(c)".
      List<String> params = element.params == null ? List.of() : element.params;
      if (element.columnName == null
          || !params.stream().allMatch(p -> p.matches("\\(\\d+\\)|(?i)ASC|DESC"))) {
        return "indexes an expression, which this test can't size";
      }
      String column = table + "." + unquoted(element.columnName);
      Matcher line = columnLine(before, column);
      if (line == null) {
        return "indexes " + column + ", which the schema before the step doesn't have";
      }
      Long prefixBytes =
          params.stream()
              .filter(p -> p.startsWith("("))
              .map(p -> 4 * Long.parseLong(p.substring(1, p.length() - 1)))
              .findFirst()
              .orElse(null);
      Long bytes = columnEntryBytes(kind, line, prefixBytes);
      if (bytes == null) {
        return "indexes " + column + " (" + line.group(3) + "), which this test can't size";
      }
      indexEntryBytes += bytes;
    }
    return overLimit(indexEntryBytes);
  }

  private static final Pattern INDEX_LINE =
      Pattern.compile("index (\\S+)\\.(\\S+) unique=\\S+ column=(\\S+)#\\d+.*");

  /**
   * Why rows could outgrow an index of the schema {@code after} a step, sized with the new type of
   * a column the step changed: PostgreSQL rebuilds the index for a type change such as int4 to
   * int8, and later rows can hold the longer values. One entry per index; a MySQL prefix index is
   * sized on the whole column, since the snapshot doesn't record the prefix.
   */
  private static List<String> regrownIndexes(
      DatabaseKind kind, SchemaSnapshot before, SchemaSnapshot after) {
    // A column grows an entry only through its type, size or scale; NOT NULL, the default and
    // the collation don't change how many bytes a value takes.
    Set<String> changed = new HashSet<>();
    for (String entry : after.entries()) {
      Matcher line = COLUMN_LINE.matcher("+ " + entry);
      if (!line.matches()) {
        continue;
      }
      Matcher old = columnLine(before, line.group(2));
      if (old != null && !sizing(old).equals(sizing(line))) {
        changed.add(line.group(2));
      }
    }
    Map<String, List<String>> columnsByIndex = new TreeMap<>();
    for (String entry : after.entries()) {
      Matcher index = INDEX_LINE.matcher(entry);
      if (index.matches()) {
        columnsByIndex
            .computeIfAbsent(index.group(1) + "." + index.group(2), name -> new ArrayList<>())
            .add(index.group(1) + "." + index.group(3));
      }
    }
    List<String> problems = new ArrayList<>();
    columnsByIndex.forEach(
        (index, columns) -> {
          if (columns.stream().noneMatch(changed::contains)) {
            return;
          }
          long indexEntryBytes = 8; // the entry's header
          for (String column : columns) {
            Matcher line = columnLine(after, column);
            Long bytes = line == null ? null : columnEntryBytes(kind, line, null);
            if (bytes == null) {
              problems.add("index " + index + " holds " + column + ", which this test can't size");
              return;
            }
            indexEntryBytes += bytes;
          }
          String problem = overLimit(indexEntryBytes);
          if (problem != null) {
            problems.add("index " + index + " " + problem);
          }
        });
    return problems;
  }

  /** The type, size and scale of a column line: what sets the bytes a value takes. */
  private static List<String> sizing(Matcher line) {
    return Arrays.asList(line.group(3), line.group(4), line.group(5));
  }

  /** The column line of {@code column} ({@code table.name}) in {@code snapshot}, or null. */
  private static Matcher columnLine(SchemaSnapshot snapshot, String column) {
    return snapshot.entries().stream()
        .map(entry -> COLUMN_LINE.matcher("+ " + entry))
        .filter(m -> m.matches() && m.group(2).equals(column))
        .findFirst()
        .orElse(null);
  }

  /**
   * The bytes the column of {@code line} takes in an index entry, or the {@code prefixBytes} of a
   * MySQL prefix when not null; null for a type this test can't bound.
   */
  private static Long columnEntryBytes(DatabaseKind kind, Matcher line, Long prefixBytes) {
    Long bytes =
        prefixBytes != null
            ? prefixBytes
            : maxBytes(kind, line.group(3), Long.parseLong(line.group(4)));
    return bytes == null ? null : entryBytes(bytes, isVariableWidth(kind, line.group(3)));
  }

  private static String overLimit(long indexEntryBytes) {
    return indexEntryBytes > MAX_INDEX_ENTRY_BYTES
        ? "has an index entry of up to "
            + indexEntryBytes
            + " bytes, over the "
            + MAX_INDEX_ENTRY_BYTES
            + " bytes PostgreSQL allows"
        : null;
  }

  private static String unquoted(String name) {
    return name.replaceAll("[\"`]", "").toLowerCase(Locale.ROOT);
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
