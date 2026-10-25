package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.migrate.MigratorTest.execute;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.migrate.kit.TestServer;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * A change made by hand to a table that this release's steps change stops the server; this
 * release's steps are those above 0.7.0's, so on every database they change {@code
 * uc_function_params} (step 2).
 */
public class ManualSchemaChangeTest {

  private static final String ADD_COLUMN =
      "ALTER TABLE uc_function_params ADD COLUMN note VARCHAR(64)";

  private static final String WHAT_TO_DO =
      "Undo the manual change, or restore the backup; see docs/server/upgrades.md (section \"The"
          + " schema differs\").";

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void startsOnACurrentDatabase() {
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    migrator.migrate();

    assertThat(migrator.prepareForStart()).isEqualTo(SchemaState.CURRENT);
    assertThat(migrator.checkSchema(new PrintStream(new ByteArrayOutputStream()))).isZero();
  }

  @Test
  void refusesToStartWithAColumnAddedByHand() {
    new SchemaMigrator(db.dataSource(), db.kind()).migrate();
    execute(db, ADD_COLUMN);

    assertThatThrownBy(() -> TestServer.start(db))
        .hasMessageContaining(
            "The schema of uc_function_params differs from what this release's steps build"
                + " (-: what they build, +: what the database has):\n"
                + "  +column uc_function_params.note type=")
        .hasMessageEndingWith(WHAT_TO_DO);
  }

  @Test
  void refusesAfterTheStepsRanOnATableChangedByHand() {
    MigratorTest.create07Schema(db);
    execute(db, ADD_COLUMN);
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());

    assertThatThrownBy(migrator::migrate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("+column uc_function_params.note type=");
    assertThat(migrator.currentVersion()).isEqualTo(migrator.latestVersion());
  }

  @Test
  void refusesToStartWithAnIndexDroppedByHand() {
    // No release before this build's: every step is this release's.
    SchemaMigrator migrator =
        new SchemaMigrator(db.dataSource(), db.kind(), List.of(), new Properties());
    migrator.migrate();
    execute(
        db,
        "DROP INDEX uc_registered_models_name_idx"
            + (db.kind() == DatabaseKind.MYSQL ? " ON uc_registered_models" : ""));

    assertThatThrownBy(migrator::prepareForStart)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The schema of uc_registered_models differs from what this release's steps build"
                + " (-: what they build, +: what the database has):\n"
                + "  -index uc_registered_models.uc_registered_models_name_idx unique=false"
                + " column=name#1\n"
                + WHAT_TO_DO);
  }

  @Test
  void listsTheFirst20Lines() {
    new SchemaMigrator(db.dataSource(), db.kind()).migrate();
    execute(db, "DROP TABLE uc_function_params");

    assertThatThrownBy(new SchemaMigrator(db.dataSource(), db.kind())::prepareForStart)
        .hasMessageContaining("  -column uc_function_params.id ")
        .hasMessageMatching("(?s).*\n  \\.\\.\\. and \\d+ more\n.*");
  }

  @Test
  void checkSchemaReportsTheDifference() {
    new SchemaMigrator(db.dataSource(), db.kind()).migrate();
    execute(db, ADD_COLUMN);
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    assertThat(
            new SchemaMigrator(db.dataSource(), db.kind())
                .checkSchema(new PrintStream(out, true, StandardCharsets.UTF_8)))
        .isOne();
    assertThat(out.toString(StandardCharsets.UTF_8))
        .contains("The schema of uc_function_params differs")
        .contains("+column uc_function_params.note type=");
  }

  /**
   * MySQL's step 3 converts every table's character set, which the schema check doesn't compare:
   * that doesn't make an index added by hand to a table it converts a difference.
   */
  @Test
  void startsWithAnIndexAddedByHandOnATableWhoseCharacterSetAStepConverts() {
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    migrator.migrate();
    execute(db, "CREATE INDEX uc_catalogs_name_id_idx ON uc_catalogs (name, id)");

    assertThat(migrator.prepareForStart()).isEqualTo(SchemaState.CURRENT);
  }

  /** Not checked: a table that no step of this release changes. */
  @Test
  void startsWithAChangeByHandOnATableThisReleaseDoesNotChange() {
    new SchemaMigrator(db.dataSource(), db.kind()).migrate();
    execute(db, "ALTER TABLE uc_catalogs ADD COLUMN note VARCHAR(64)");
    // 0.7.0 ends at step 3; this release's own step 5001 creates test_release_items.
    SchemaMigrator migrator =
        new SchemaMigrator(
            db.dataSource(),
            db.kind(),
            List.of("classpath:db/test-steps/release-first/" + db.kind().folder()),
            SkippedReleaseTest.RELEASES);

    assertThat(migrator.prepareForStart()).isEqualTo(SchemaState.CURRENT);
    assertThat(migrator.checkSchema(new PrintStream(new ByteArrayOutputStream()))).isZero();
  }
}
