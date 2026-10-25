package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.migrate.MigratorTest.execute;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.migrate.SchemaMigrator.MigrationOutcome;
import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test-only steps create {@code test_repeat_items} (version 4000) and then change the database once
 * per kind of change a SQL step can find already there (from version 4001). The change of a step is
 * applied by hand, as a run that stopped before Flyway recorded the step leaves it.
 */
public class StepAlreadyInDatabaseTest {

  private static final String BASE = "classpath:db/test-steps/repeat-base/";
  private static final String STEPS = "classpath:db/test-steps/repeat/";
  // Version 4100 adds two columns in one statement; H2 has no form with one ADD per column, so its
  // step adds them as one list.
  private static final String TWO_COLUMNS = "db/test-steps/repeat-two-columns/";
  // Version 4200 drops the size of a varchar (H2 and PostgreSQL; MySQL's varchar takes a size).
  private static final String UNSIZED = "db/test-steps/repeat-unsized/";

  @TempDatabase(empty = true)
  TestDatabase db;

  /** The steps from version 4001, one per kind of change. */
  static List<String> steps() {
    return List.of(
        "V4001__create_test_repeat_others.sql",
        "V4002__add_test_repeat_item_note.sql",
        "V4003__index_test_repeat_item_names.sql",
        "V4004__unique_test_repeat_item_codes.sql",
        "V4005__widen_test_repeat_item_labels.sql");
  }

  @ParameterizedTest
  @MethodSource("steps")
  void recordsAStepWhoseChangeIsThereWithoutRunningIt(String step) {
    migrator(db.dataSource(), BASE).migrate();
    execute(db, statement(step));
    List<String> sent = new ArrayList<>();
    SchemaMigrator migrator = migrator(recording(sent), BASE, STEPS);
    String latest = migrator.latestVersion();
    int steps = Integer.parseInt(latest) - 4000;

    assertThat(migrator.migrate()).isEqualTo(new MigrationOutcome("4000", latest, steps));
    String version = step.substring(1, step.indexOf("__"));
    assertThat(migrator.applied())
        .filteredOn(m -> m.getVersion().getVersion().equals(version))
        .extracting(MigrationInfo::getState)
        .containsExactly(MigrationState.SUCCESS);
    assertThat(sent).doesNotContain(statement(step));
  }

  @Test
  void runsEveryStepWhoseChangeIsNotThere() {
    migrator(db.dataSource(), BASE).migrate();
    List<String> sent = new ArrayList<>();
    SchemaMigrator migrator = migrator(recording(sent), BASE, STEPS);

    migrator.migrate();

    assertThat(sent).containsAll(steps().stream().map(this::statement).toList());
  }

  @Test
  void runsAStepOfTwoChangesWhenOneIsThere() {
    migrator(db.dataSource(), BASE).migrate();
    execute(db, "ALTER TABLE test_repeat_items ADD extra_a INT");
    List<String> sent = new ArrayList<>();
    SchemaMigrator migrator = migrator(recording(sent), BASE, "classpath:" + TWO_COLUMNS);

    assertThatThrownBy(migrator::migrate).hasMessageContaining("V4100");
    assertThat(sent)
        .contains(statement(TWO_COLUMNS, "V4100__add_two_test_repeat_item_columns.sql"));
  }

  @Test
  void runsAStepThatDropsTheSizeOfAType() {
    assumeTrue(db.kind() != DatabaseKind.MYSQL);
    migrator(db.dataSource(), BASE).migrate();
    List<String> sent = new ArrayList<>();

    migrator(recording(sent), BASE, "classpath:" + UNSIZED).migrate();

    assertThat(sent).contains(statement(UNSIZED, "V4200__unbound_test_repeat_item_names.sql"));
  }

  @Test
  void adoptsA07DatabaseWhoseWideningIsAlreadyThere() {
    MigratorTest.create07Schema(db);
    String widening = statement("db/migration/", "V2__widen_function_param_type_json.sql");
    execute(db, widening);
    List<String> sent = new ArrayList<>();
    SchemaMigrator migrator = new SchemaMigrator(recording(sent), db.kind());

    assertThat(migrator.migrate())
        .isEqualTo(
            new MigrationOutcome(
                "1", migrator.latestVersion(), MigratorTest.releaseSteps(migrator) - 1));
    assertThat(migrator.applied().get(0).getType().isBaseline()).isTrue();
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
    assertThat(sent).doesNotContain(widening);
  }

  private SchemaMigrator migrator(DataSource dataSource, String... steps) {
    return new SchemaMigrator(
        dataSource, db.kind(), List.of(steps).stream().map(s -> s + db.kind().folder()).toList());
  }

  /** The one statement of a test-only step of this database, as Flyway sends it. */
  private String statement(String step) {
    return statement("db/test-steps/repeat/", step);
  }

  /** The one statement of the step {@code step} of this database under {@code base}. */
  private String statement(String base, String step) {
    String path = base + db.kind().folder() + "/" + step;
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(path)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8).strip().replaceAll(";$", "");
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The test database, adding the text of every statement sent through it to {@code sent}. */
  private DataSource recording(List<String> sent) {
    return recording(DataSource.class, db.dataSource(), sent);
  }

  /** Records the SQL passed to {@code target}, and to the connections and statements it returns. */
  @SuppressWarnings("unchecked")
  private static <T> T recording(Class<T> type, T target, List<String> sent) {
    return (T)
        Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) -> {
              if (args != null && args.length > 0 && args[0] instanceof String sql) {
                sent.add(sql.strip());
              }
              Object result;
              try {
                result = method.invoke(target, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              Class<?> returned = method.getReturnType();
              boolean wrap =
                  Connection.class.isAssignableFrom(returned)
                      || Statement.class.isAssignableFrom(returned);
              return wrap && result != null
                  ? recording((Class<Object>) returned, result, sent)
                  : result;
            });
  }
}
