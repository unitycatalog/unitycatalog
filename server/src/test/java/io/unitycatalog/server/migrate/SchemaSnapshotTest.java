package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SchemaSnapshotTest {

  @TempDatabase(empty = true)
  TestDatabase before;

  @TempDatabase(empty = true)
  TestDatabase after;

  /**
   * A one-statement step can change each of these, so two schemas that differ only there must give
   * different snapshots: otherwise the state check takes a step that did not run as applied.
   */
  @ParameterizedTest(name = "{0}")
  @CsvSource(
      delimiter = '|',
      value = {
        "numeric scale | H2 POSTGRESQL MYSQL"
            + " | CREATE TABLE uc_t (a DECIMAL(10, 2))"
            + " | CREATE TABLE uc_t (a DECIMAL(10, 4))",
        "foreign key delete rule | H2 POSTGRESQL MYSQL"
            + " | CREATE TABLE uc_t (id INT PRIMARY KEY, p INT, CONSTRAINT fk FOREIGN KEY (p)"
            + " REFERENCES uc_t (id))"
            + " | CREATE TABLE uc_t (id INT PRIMARY KEY, p INT, CONSTRAINT fk FOREIGN KEY (p)"
            + " REFERENCES uc_t (id) ON DELETE CASCADE)",
        "foreign key update rule | H2 POSTGRESQL MYSQL"
            + " | CREATE TABLE uc_t (id INT PRIMARY KEY, p INT, CONSTRAINT fk FOREIGN KEY (p)"
            + " REFERENCES uc_t (id))"
            + " | CREATE TABLE uc_t (id INT PRIMARY KEY, p INT, CONSTRAINT fk FOREIGN KEY (p)"
            + " REFERENCES uc_t (id) ON UPDATE CASCADE)",
        "index order | H2 POSTGRESQL MYSQL"
            + " | CREATE TABLE uc_t (a INT); CREATE INDEX ix ON uc_t (a)"
            + " | CREATE TABLE uc_t (a INT); CREATE INDEX ix ON uc_t (a DESC)",
        "enum values | H2 MYSQL"
            + " | CREATE TABLE uc_t (a ENUM('X', 'Y'))"
            + " | CREATE TABLE uc_t (a ENUM('X', 'Y', 'Z'))",
        "enum value case | H2 MYSQL"
            + " | CREATE TABLE uc_t (a ENUM('X', 'Y'))"
            + " | CREATE TABLE uc_t (a ENUM('x', 'y'))",
        "collation | MYSQL"
            + " | CREATE TABLE uc_t (a VARCHAR(10))"
            + " | CREATE TABLE uc_t (a VARCHAR(10) COLLATE utf8mb4_bin)",
        "collation | POSTGRESQL"
            + " | CREATE TABLE uc_t (a VARCHAR(10))"
            + " | CREATE TABLE uc_t (a VARCHAR(10) COLLATE \"C\")",
        "partial index | POSTGRESQL"
            + " | CREATE TABLE uc_t (a INT); CREATE INDEX ix ON uc_t (a)"
            + " | CREATE TABLE uc_t (a INT); CREATE INDEX ix ON uc_t (a) WHERE a > 0",
      })
  void tellsApartSchemasThatDifferOnlyIn(
      String difference, String databases, String beforeSql, String afterSql) {
    DatabaseKind kind = DatabaseKind.of(before.dataSource());
    assumeTrue(Arrays.asList(databases.split(" ")).contains(kind.name()), "not on " + kind);
    UpgradeTestUtils.executeStatements(before, beforeSql);
    UpgradeTestUtils.executeStatements(after, afterSql);

    assertThat(SchemaSnapshot.of(after.dataSource()))
        .isNotEqualTo(SchemaSnapshot.of(before.dataSource()));
  }

  @Test
  void appliesAnEffectFileWithNoChanges() {
    // The effect file of a step that changes nothing the snapshot shows holds one empty line.
    SchemaSnapshot schema = SchemaSnapshot.of(before.dataSource());

    assertThat(schema.with("\n".lines().toList())).isEqualTo(schema);
  }

  @Test
  void countsDuplicateChecksAsOne() {
    assumeTrue(DatabaseKind.of(before.dataSource()) == DatabaseKind.H2, "H2 only");
    UpgradeTestUtils.execute(before, "CREATE TABLE uc_t (a INT, CONSTRAINT c1 CHECK (a > 0))");
    UpgradeTestUtils.execute(
        after,
        "CREATE TABLE uc_t (a INT, CONSTRAINT c1 CHECK (a > 0), CONSTRAINT c2 CHECK (a > 0))");

    assertThat(SchemaSnapshot.of(after.dataSource()))
        .isEqualTo(SchemaSnapshot.of(before.dataSource()));
  }
}
