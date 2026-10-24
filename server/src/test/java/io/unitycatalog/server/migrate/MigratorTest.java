package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.flywaydb.core.api.MigrationInfo;
import org.junit.jupiter.api.Test;

public class MigratorTest {

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void setsUpAnEmptyDatabase() {
    Migrator migrator = new Migrator(db.dataSource(), db.kind());
    assertThat(migrator.state()).isEqualTo(SchemaState.EMPTY);

    MigrationOutcome outcome = migrator.migrate();

    assertThat(outcome.to()).isEqualTo(migrator.latestVersion());
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void setsUpADatabaseWithOnlyTheAuthorizationRulesTable() {
    execute(db, "CREATE TABLE casbin_rule (id INT)");
    Migrator migrator = new Migrator(db.dataSource(), db.kind());
    assertThat(migrator.state()).isEqualTo(SchemaState.EMPTY);

    assertThat(migrator.migrate().applied()).isEqualTo(releaseSteps(migrator));
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void adoptsA07Database() {
    create07Schema(db);
    Migrator migrator = new Migrator(db.dataSource(), db.kind());
    assertThat(migrator.state()).isEqualTo(SchemaState.UNVERSIONED_UC);

    MigrationOutcome outcome = migrator.migrate();

    String latest = migrator.latestVersion();
    assertThat(outcome).isEqualTo(new MigrationOutcome("1", latest, releaseSteps(migrator) - 1));
    MigrationInfo baseline = migrator.applied().get(0);
    assertThat(baseline.getType().isBaseline()).isTrue();
    assertThat(baseline.getVersion().getVersion()).isEqualTo("1");
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void makesTextComparisonsCaseSensitive() throws SQLException {
    if (db.kind() == DatabaseKind.MYSQL) {
      // A 0.7 install on MySQL's default collation, which ignores case.
      execute(db, "ALTER DATABASE CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci");
    }
    create07Schema(db);
    try (Connection connection = db.dataSource().getConnection();
        PreparedStatement insert =
            connection.prepareStatement("INSERT INTO uc_catalogs (id, name) VALUES (?, 'Sales')")) {
      insert.setObject(1, db.kind() == DatabaseKind.MYSQL ? new byte[16] : new UUID(0, 1));
      insert.executeUpdate();
    }

    new Migrator(db.dataSource(), db.kind()).migrate();

    assertThat(catalogsNamed(db, "sales")).isZero();
    assertThat(catalogsNamed(db, "Sales")).isOne();
  }

  @Test
  void refusesADatabaseOfAnotherApplication() {
    execute(db, "CREATE TABLE orders (id INT)");
    Migrator migrator = new Migrator(db.dataSource(), db.kind());
    assertThat(migrator.state()).isEqualTo(SchemaState.NOT_UC);

    assertThatThrownBy(migrator::migrate).hasMessageContaining("not a Unity Catalog database");
  }

  @Test
  void secondRunAppliesNothing() {
    Migrator migrator = new Migrator(db.dataSource(), db.kind());
    migrator.migrate();

    assertThat(migrator.migrate().applied()).isZero();
  }

  @Test
  void refusesASchemaNewerThanThisRelease() {
    new Migrator(db.dataSource(), db.kind(), List.of("classpath:db/test-steps/ahead")).migrate();
    Migrator migrator = new Migrator(db.dataSource(), db.kind());
    assertThat(migrator.state()).isEqualTo(SchemaState.AHEAD);

    assertThatThrownBy(migrator::migrate).hasMessageContaining("newer than this release");
  }

  @Test
  void reportsAFailedStep() {
    Migrator migrator = new Migrator(db.dataSource(), db.kind());
    migrator.migrate();
    String q = quote(db);
    execute(
        db,
        ("INSERT INTO %1$sflyway_schema_history%1$s (%1$sinstalled_rank%1$s, %1$sversion%1$s,"
                + " %1$sdescription%1$s, %1$stype%1$s, %1$sscript%1$s, %1$sinstalled_by%1$s,"
                + " %1$sexecution_time%1$s, %1$ssuccess%1$s)"
                + " VALUES (1000, '1000', 'broken', 'SQL', 'V1000__broken.sql', 'test', 0, false)")
            .formatted(q));

    assertThat(migrator.state()).isEqualTo(SchemaState.FAILED);
  }

  @Test
  void concurrentRunsApplyEachStepOnce() throws Exception {
    CyclicBarrier start = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    List<MigrationOutcome> outcomes = new ArrayList<>();
    try {
      List<Future<MigrationOutcome>> runs = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        runs.add(
            executor.submit(
                () -> {
                  start.await();
                  return new Migrator(db.dataSource(), db.kind()).migrate();
                }));
      }
      for (Future<MigrationOutcome> run : runs) {
        outcomes.add(run.get(2, TimeUnit.MINUTES));
      }
    } finally {
      executor.shutdownNow();
    }

    // The second run waits for the first, then finds the schema current.
    Migrator migrator = new Migrator(db.dataSource(), db.kind());
    String latest = migrator.latestVersion();
    int steps = releaseSteps(migrator);
    assertThat(outcomes)
        .containsExactlyInAnyOrder(
            new MigrationOutcome(null, latest, steps), new MigrationOutcome(latest, latest, 0));
    assertThat(historyVersions(db))
        .containsExactlyElementsOf(
            IntStream.rangeClosed(1, steps).mapToObj(String::valueOf).toList());
  }

  /** The number of steps in this release: they are numbered 1, 2, 3 and so on. */
  static int releaseSteps(Migrator migrator) {
    return Integer.parseInt(migrator.latestVersion());
  }

  /** Builds a 0.7 database: that release's schema without Flyway's history. */
  static void create07Schema(TestDatabase db) {
    String path = "db/migration/" + db.kind().folder() + "/V1__baseline_0_7.sql";
    String sql;
    try (InputStream in = MigratorTest.class.getClassLoader().getResourceAsStream(path)) {
      sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    String withoutComments = sql.replaceAll("(?m)^\\s*--.*$", "");
    for (String statement : withoutComments.split(";")) {
      if (!statement.isBlank()) {
        execute(db, statement);
      }
    }
  }

  private static int catalogsNamed(TestDatabase db, String name) throws SQLException {
    try (Connection connection = db.dataSource().getConnection();
        PreparedStatement query =
            connection.prepareStatement("SELECT COUNT(*) FROM uc_catalogs WHERE name = ?")) {
      query.setString(1, name);
      try (ResultSet rs = query.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  /** The versions in Flyway's history (its own marker rows have none). */
  private static List<String> historyVersions(TestDatabase db) throws SQLException {
    String q = quote(db);
    List<String> versions = new ArrayList<>();
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                ("SELECT %1$sversion%1$s FROM %1$sflyway_schema_history%1$s"
                        + " WHERE %1$sversion%1$s IS NOT NULL ORDER BY %1$sversion%1$s")
                    .formatted(q))) {
      while (rs.next()) {
        versions.add(rs.getString(1));
      }
    }
    return versions;
  }

  /** Flyway creates its history table with quoted lower-case names. */
  private static String quote(TestDatabase db) {
    try (Connection connection = db.dataSource().getConnection()) {
      return connection.getMetaData().getIdentifierQuoteString();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void execute(TestDatabase db, String sql) {
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new IllegalStateException(sql, e);
    }
  }
}
