package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.migrate.MigratorTest.execute;
import static io.unitycatalog.server.migrate.MigratorTest.quote;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.migrate.SchemaMigrator.MigrationOutcome;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * A test-only release map: 0.7.0 ends at step 1 and 0.7.5, the release before this build, at step
 * 3. This release's test-only steps create {@code test_release_items} (version 5001) and add a
 * column to it (version 5002).
 */
public class SkippedReleaseTest {

  static final Properties RELEASES = releases("0.7.0=1", "0.7.5=3");

  private static final String FIRST_STEP = "classpath:db/test-steps/release-first/";
  private static final String SECOND_STEP = "classpath:db/test-steps/release-second/";

  private static final String REFUSAL =
      "The database is at schema version 2, from release 0.7.0 or earlier; this release upgrades"
          + " only from 0.7.5 (version 3 or later). Upgrade to 0.7.5 first, then to this release.";

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void upgradesADatabaseOfThePreviousRelease() {
    new SchemaMigrator(db.dataSource(), db.kind()).migrate();

    assertThat(migrator().migrate()).isEqualTo(new MigrationOutcome("3", "5002", 2));
  }

  @Test
  void finishesAnUpgradeThatStoppedMidway() {
    new SchemaMigrator(db.dataSource(), db.kind(), List.of(FIRST_STEP + db.kind().folder()))
        .migrate();

    assertThat(migrator().migrate()).isEqualTo(new MigrationOutcome("5001", "5002", 1));
  }

  @Test
  void setsUpAnEmptyDatabase() {
    assertThat(migrator().migrate().to()).isEqualTo("5002");
  }

  @Test
  void adoptsADatabaseOfTheBaselineRelease() {
    MigratorTest.create07Schema(db);

    assertThat(migrator().migrate()).isEqualTo(new MigrationOutcome("1", "5002", 4));
  }

  @Test
  void refusesADatabaseOfAnOlderReleaseAndChangesNothing() {
    migrateToVersion2();
    SchemaMigrator migrator = migrator();
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());

    assertThatThrownBy(migrator::migrate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(REFUSAL);
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);
    assertThat(migrator.currentVersion()).isEqualTo("2");
  }

  @Test
  void checkSchemaReportsASkippedRelease() {
    migrateToVersion2();
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    assertThat(migrator().checkSchema(new PrintStream(out, true, StandardCharsets.UTF_8))).isOne();
    assertThat(out.toString(StandardCharsets.UTF_8)).contains(REFUSAL);
  }

  /** A database whose history ends at step 2, as release 0.7.0 then a release with step 2. */
  private void migrateToVersion2() {
    new SchemaMigrator(db.dataSource(), db.kind()).migrate();
    execute(
        db,
        "DELETE FROM %1$suc_schema_history%1$s WHERE %1$sversion%1$s = '3'".formatted(quote(db)));
  }

  private SchemaMigrator migrator() {
    return new SchemaMigrator(
        db.dataSource(),
        db.kind(),
        List.of(FIRST_STEP + db.kind().folder(), SECOND_STEP + db.kind().folder()),
        RELEASES);
  }

  /** A release map of {@code release=last step} lines. */
  static Properties releases(String... lines) {
    Properties releases = new Properties();
    try {
      releases.load(new StringReader(String.join("\n", lines)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return releases;
  }
}
