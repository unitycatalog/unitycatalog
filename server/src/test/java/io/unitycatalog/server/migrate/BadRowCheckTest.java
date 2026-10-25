package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.migrate.MigratorTest.execute;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.migrate.SchemaMigrator.MigrationOutcome;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * Test-only steps create {@code test_named_items} (version 3000) and make its names unique (version
 * 3001), whose check lists the rows of names used more than once.
 */
public class BadRowCheckTest {

  private static final String CREATE_TABLE = "classpath:db/test-steps/named-items/";
  private static final String UNIQUE_NAMES = "classpath:db/test-steps/unique-names/";

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void refusesTheUpgradeWhileACheckFindsRows() {
    createItems("a", "a", "b");
    SchemaMigrator migrator = migrator(CREATE_TABLE, UNIQUE_NAMES);
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());
    List<String> history = history(migrator);

    assertThatThrownBy(migrator::migrate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The upgrade is stopped before any change: 1 check found rows that break a rule of"
                + " this release.\n"
                + "Check 3001 (unique test named item names) found 2 rows:\n"
                + "  id=1, name=a\n"
                + "  id=2, name=a\n"
                + "Fix them as docs/server/upgrades.md shows (section \"Rows that block an"
                + " upgrade\"), then start the server again.");
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);
    assertThat(history(migrator)).isEqualTo(history);
    assertThat(migrator.currentVersion()).isEqualTo("3000");
  }

  @Test
  void upgradesOnceTheRowsAreFixed() {
    createItems("a", "a", "b");
    execute(db, "DELETE FROM test_named_items WHERE id = 2");

    assertThat(migrator(CREATE_TABLE, UNIQUE_NAMES).migrate())
        .isEqualTo(new MigrationOutcome("3000", "3001", 1));
  }

  /** The server's pool doesn't autocommit (see {@link HibernateConfigurator#hikariConfig}). */
  @Test
  void checksAndUpgradesOnTheServersPool() {
    createItems("a", "a", "b");
    try (HikariDataSource pool =
        new HikariDataSource(HibernateConfigurator.hikariConfig(db.hibernateProperties()))) {
      SchemaMigrator migrator = migrator(pool, CREATE_TABLE, UNIQUE_NAMES);

      assertThatThrownBy(migrator::migrate)
          .hasMessageContaining("Check 3001 (unique test named item names) found 2 rows:\n");

      execute(db, "DELETE FROM test_named_items WHERE id = 2");
      assertThat(migrator.migrate()).isEqualTo(new MigrationOutcome("3000", "3001", 1));
    }
    assertThat(migrator(CREATE_TABLE, UNIQUE_NAMES).currentVersion()).isEqualTo("3001");
  }

  @Test
  void listsTheFirst20RowsOfACheck() {
    createItems(Collections.nCopies(25, "a").toArray(String[]::new));

    assertThatThrownBy(migrator(CREATE_TABLE, UNIQUE_NAMES)::migrate)
        .hasMessageContaining("found 25 rows:\n")
        .hasMessageContaining("  id=20, name=a\n  ... and 5 more\n")
        .hasMessageNotContaining("id=21");
  }

  @Test
  void checkSchemaListsTheRowsAndChangesNothing() {
    createItems("a", "a", "b");
    SchemaMigrator migrator = migrator(CREATE_TABLE, UNIQUE_NAMES);
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());
    List<String> history = history(migrator);
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    assertThat(migrator.checkSchema(new PrintStream(out, true, StandardCharsets.UTF_8))).isOne();
    assertThat(out.toString(StandardCharsets.UTF_8))
        .contains("Check 3001 (unique test named item names) found 2 rows:\n")
        .contains("  id=1, name=a\n  id=2, name=a\n");
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);
    assertThat(history(migrator)).isEqualTo(history);
  }

  @Test
  void checkSchemaPassesOnceTheRowsAreFixed() {
    createItems("a", "a", "b");
    execute(db, "DELETE FROM test_named_items WHERE id = 2");
    SchemaMigrator migrator = migrator(CREATE_TABLE, UNIQUE_NAMES);

    assertThat(migrator.checkSchema(new PrintStream(new ByteArrayOutputStream()))).isZero();
    assertThat(migrator.currentVersion()).isEqualTo("3000");
  }

  @Test
  void runsNoCheckOnAnEmptyDatabase() {
    assertThat(migrator(CREATE_TABLE, UNIQUE_NAMES).migrate().to()).isEqualTo("3001");
  }

  /** Migrates to version 3000 and inserts an item per name, numbered from 1. */
  private void createItems(String... names) {
    migrator(CREATE_TABLE).migrate();
    for (int i = 0; i < names.length; i++) {
      execute(
          db,
          "INSERT INTO test_named_items (id, name) VALUES (%d, '%s')".formatted(i + 1, names[i]));
    }
  }

  private SchemaMigrator migrator(String... steps) {
    return migrator(db.dataSource(), steps);
  }

  private SchemaMigrator migrator(DataSource dataSource, String... steps) {
    return new SchemaMigrator(
        dataSource, db.kind(), List.of(steps).stream().map(s -> s + db.kind().folder()).toList());
  }

  private static List<String> history(SchemaMigrator migrator) {
    return migrator.applied().stream().map(m -> m.getVersion() + " " + m.getState()).toList();
  }
}
