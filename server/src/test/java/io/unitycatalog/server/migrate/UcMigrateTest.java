package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

public class UcMigrateTest {

  @TempDatabase(empty = true)
  TestDatabase db;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();

  @Test
  void infoShowsTheStateAndThePendingSteps() {
    MigratorTest.create07Schema(db);

    assertThat(run("info")).isZero();

    assertThat(out())
        .contains("State: UNVERSIONED_UC")
        .contains("Current version: none")
        .contains("Latest version: " + latestVersion())
        .contains("2  widen function param type json")
        .doesNotContain("1  baseline 0 7");
  }

  @Test
  void migrateAdoptsA07Database() {
    MigratorTest.create07Schema(db);

    assertThat(run("migrate")).isZero();

    String latest = latestVersion();
    assertThat(out())
        .contains(
            "Schema version 1 -> " + latest + " (" + (Integer.parseInt(latest) - 1) + " steps)");
  }

  @Test
  void migrateRefusesADatabaseOfAnotherApplication() throws Exception {
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE orders (id INT)");
    }

    assertThat(run("migrate")).isEqualTo(1);

    assertThat(err.toString(StandardCharsets.UTF_8)).contains("not a Unity Catalog database");
  }

  private int run(String command) {
    return UcMigrate.run(
        new String[] {command},
        db.hibernateProperties(),
        new PrintStream(out, true, StandardCharsets.UTF_8),
        new PrintStream(err, true, StandardCharsets.UTF_8));
  }

  private String latestVersion() {
    return new Migrator(db.dataSource(), db.kind()).latestVersion();
  }

  private String out() {
    return out.toString(StandardCharsets.UTF_8);
  }
}
