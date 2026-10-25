package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.migrate.MigratorTest.execute;
import static io.unitycatalog.server.migrate.MigratorTest.quote;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.migrate.SchemaMigrator.MigrationOutcome;
import io.unitycatalog.server.migrate.kit.TestServer;
import io.unitycatalog.server.migrate.teststeps.batched.V2001__Fill_upper_name_in_batches;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.File;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.api.MigrationState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test-only steps create {@code test_data_step_items} (version 2000, SQL) and fill its {@code
 * upper_name} from {@code name} (version 2001, a {@link DataStep}), in one transaction, in batches
 * of 2, or slowly in batches of 1.
 */
public class DataStepTest {

  private static final int ROWS = 5;
  private static final String CREATE_TABLE = "classpath:db/test-steps/data/";
  private static final String IN_ONE_TRANSACTION =
      "classpath:io/unitycatalog/server/migrate/teststeps/onetx";
  private static final String IN_BATCHES =
      "classpath:io/unitycatalog/server/migrate/teststeps/batched";
  private static final String SLOWLY =
      "classpath:io/unitycatalog/server/migrate/teststeps/slowbatches";

  @TempDatabase(empty = true)
  TestDatabase db;

  /** Migrates to version 2000 (the table) and inserts the rows the data steps fill. */
  @BeforeEach
  void createRows() throws SQLException {
    migrate();
    try (Connection connection = db.dataSource().getConnection();
        PreparedStatement insert =
            connection.prepareStatement(
                "INSERT INTO test_data_step_items (id, name) VALUES (?, ?)")) {
      for (int i = 1; i <= ROWS; i++) {
        insert.setInt(1, i);
        insert.setString(2, "item_" + i);
        insert.executeUpdate();
      }
    }
  }

  @Test
  void fillsEveryRowInOneTransaction() throws SQLException {
    assertThat(migrate(IN_ONE_TRANSACTION)).isEqualTo(new MigrationOutcome("2000", "2001", 1));

    assertThat(filledRows()).isEqualTo(ROWS);
  }

  @Test
  void fillsEveryRowInBatches() throws SQLException {
    assertThat(migrate(IN_BATCHES)).isEqualTo(new MigrationOutcome("2000", "2001", 1));

    assertThat(filledRows()).isEqualTo(ROWS);
  }

  @Test
  void rerunChangesNothing() throws SQLException {
    migrate(IN_BATCHES);
    execute(db, "UPDATE test_data_step_items SET upper_name = NULL WHERE id = 1");

    assertThat(migrate(IN_BATCHES).applied()).isZero();
    assertThat(filledRows()).isEqualTo(ROWS - 1);
  }

  @Test
  void stepAlreadyDoneIsRecordedWithoutRunning() throws SQLException {
    execute(db, "UPDATE test_data_step_items SET upper_name = 'by hand'");

    assertThat(migrate(IN_ONE_TRANSACTION)).isEqualTo(new MigrationOutcome("2000", "2001", 1));
    assertThat(filledRows()).isZero();
    assertThat(historyRows("2001")).isOne();
  }

  @Test
  void batchedStepCommitsEachBatch() throws SQLException {
    V2001__Fill_upper_name_in_batches.stopAfterFirstBatch(true);
    try {
      assertThatThrownBy(() -> migrate(IN_BATCHES))
          .rootCause()
          .hasMessageContaining("Stopped by the test after the first batch");
    } finally {
      V2001__Fill_upper_name_in_batches.stopAfterFirstBatch(false);
    }
    assertThat(filledRows()).isEqualTo(2);

    // H2 and MySQL record the failed step (PostgreSQL rolls the record back with Flyway's lock
    // transaction); either way the rerun runs it again and changes the rows the first run left.
    assertThat(migrate(IN_BATCHES)).isEqualTo(new MigrationOutcome("2000", "2001", 1));
    assertThat(filledRows()).isEqualTo(ROWS);
    assertThat(historyRows("2001")).isOne();
  }

  @Test
  void serverStopsAtABatchOnSigterm() throws Exception {
    // H2 has no sleep function for the slow step, and closes its database in a shutdown hook of
    // its own, under the running step.
    assumeTrue(db.kind() != DatabaseKind.H2);
    Path properties = Files.createTempFile("uc-hibernate", ".properties");
    try {
      try (Writer out = Files.newBufferedWriter(properties)) {
        db.hibernateProperties().store(out, null);
      }
      Process server =
          new ProcessBuilder(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  // The test's classes may record coverage; the server's process records none.
                  "-Djacoco-agent.output=none",
                  "-cp",
                  System.getProperty("java.class.path"),
                  TestServer.class.getName(),
                  properties.toString(),
                  CREATE_TABLE + db.kind().folder(),
                  SLOWLY)
              .directory(new File(System.getProperty("user.dir")))
              .inheritIO()
              .start();
      int filled;
      try {
        long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
        while ((filled = filledRows()) == 0) {
          assertThat(server.isAlive()).as("the server is running").isTrue();
          assertThat(System.nanoTime()).as("the step started in time").isLessThan(deadline);
          Thread.sleep(100);
        }
      } finally {
        server.destroy(); // SIGTERM, during the batch after the rows filled so far
        assertThat(server.waitFor(2, TimeUnit.MINUTES)).isTrue();
      }
      assertThat(server.exitValue()).isNotZero();
      // The server finished that batch (killing it would roll the batch back), then stopped.
      assertThat(filledRows()).isGreaterThan(filled).isLessThan(ROWS);
    } finally {
      Files.delete(properties);
    }
    assertThat(new SchemaMigrator(db.dataSource(), db.kind(), locations(SLOWLY)).applied())
        .filteredOn(m -> m.getVersion().getVersion().equals("2001"))
        .noneMatch(m -> m.getState() == MigrationState.SUCCESS);

    assertThat(migrate(SLOWLY)).isEqualTo(new MigrationOutcome("2000", "2001", 1));
    assertThat(filledRows()).isEqualTo(ROWS);
  }

  /** Migrates with the release steps, the table step and the given steps. */
  private MigrationOutcome migrate(String... steps) {
    try (SchemaMigrator migrator =
        new SchemaMigrator(db.dataSource(), db.kind(), locations(steps))) {
      return migrator.migrate();
    }
  }

  private List<String> locations(String... steps) {
    List<String> locations = new ArrayList<>();
    locations.add(CREATE_TABLE + db.kind().folder());
    locations.addAll(List.of(steps));
    return locations;
  }

  private int filledRows() throws SQLException {
    return count("SELECT COUNT(*) FROM test_data_step_items WHERE upper_name = UPPER(name)");
  }

  /** The history rows of {@code version}. */
  private int historyRows(String version) throws SQLException {
    return count(
        "SELECT COUNT(*) FROM %1$suc_schema_history%1$s WHERE %1$sversion%1$s = '%2$s'"
            .formatted(quote(db), version));
  }

  private int count(String sql) throws SQLException {
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      rs.next();
      return rs.getInt(1);
    }
  }
}
