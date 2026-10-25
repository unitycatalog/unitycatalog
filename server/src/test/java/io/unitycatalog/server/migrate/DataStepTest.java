package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.migrate.MigratorTest.execute;
import static io.unitycatalog.server.migrate.MigratorTest.quote;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.migrate.SchemaMigrator.MigrationOutcome;
import io.unitycatalog.server.migrate.kit.TestServer;
import io.unitycatalog.server.migrate.teststeps.batched.V2001__Fill_upper_name_in_batches;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.File;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
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
  private static final String SLOW_FINAL = "classpath:db/test-steps/slow-final/";

  /** The rows a data step filled. */
  private static final String FILLED =
      "SELECT COUNT(*) FROM test_data_step_items WHERE upper_name = UPPER(name)";

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
    int filled = stopServerDuring(SLOWLY, FILLED);

    // The server finished that batch (killing it would roll the batch back), then stopped.
    assertThat(filledRows()).isGreaterThan(filled).isLessThan(ROWS);
    assertThat(new SchemaMigrator(db.dataSource(), db.kind(), locations(SLOWLY)).applied())
        .filteredOn(m -> m.getVersion().getVersion().equals("2001"))
        .noneMatch(m -> m.getState() == MigrationState.SUCCESS);

    assertThat(migrate(SLOWLY)).isEqualTo(new MigrationOutcome("2000", "2001", 1));
    assertThat(filledRows()).isEqualTo(ROWS);
  }

  @Test
  void serverRestoresTheBackupOnSigterm() throws Exception {
    assumeTrue(db.kind() != DatabaseKind.H2); // as serverStopsAtABatchOnSigterm
    try {
      stopServerDuring(SLOWLY, FILLED, "-D" + Property.MIGRATION_BACKUP.getKey() + "=true");

      // The server restored the database from before step 2001 before it exited.
      assertThat(filledRows()).isZero();
      assertThat(historyRows("2001")).isZero();
    } finally {
      BackupTest.dropCompanions(db);
    }
  }

  /** SIGTERM during the last step, which has no boundary of its own: the server still restores. */
  @Test
  void serverRestoresTheBackupOnSigtermDuringTheLastStep() throws Exception {
    assumeTrue(db.kind() != DatabaseKind.H2); // as serverStopsAtABatchOnSigterm
    String running =
        db.kind() == DatabaseKind.POSTGRESQL
            ? "SELECT COUNT(*) FROM pg_stat_activity"
                + " WHERE query LIKE '%pg_sleep(5)%' AND pid <> pg_backend_pid()"
            : "SELECT COUNT(*) FROM information_schema.processlist"
                + " WHERE info LIKE '%SLEEP(1) = 0%' AND id <> CONNECTION_ID()";
    try {
      stopServerDuring(
          SLOW_FINAL + db.kind().folder(),
          running,
          "-D" + Property.MIGRATION_BACKUP.getKey() + "=true");

      assertThat(filledRows()).isZero();
      assertThat(historyRows("2001")).isZero();
    } finally {
      BackupTest.dropCompanions(db);
    }
  }

  /**
   * Starts a server in a process of its own, with {@code jvmArgs}, that runs the slow step in
   * {@code steps}, sends it SIGTERM once {@code progress} (a count, on a connection of its own) is
   * above 0, and waits for it to exit with a non-zero code. Returns that count.
   */
  private int stopServerDuring(String steps, String progress, String... jvmArgs) throws Exception {
    Path properties = Files.createTempFile("uc-hibernate", ".properties");
    try {
      try (Writer out = Files.newBufferedWriter(properties)) {
        db.hibernateProperties().store(out, null);
      }
      List<String> command = new ArrayList<>();
      command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
      // The test's classes may record coverage; the server's process records none.
      command.add("-Djacoco-agent.output=none");
      command.addAll(List.of(jvmArgs));
      command.addAll(
          List.of(
              "-cp",
              System.getProperty("java.class.path"),
              TestServer.class.getName(),
              properties.toString(),
              CREATE_TABLE + db.kind().folder(),
              steps));
      // With the backup on, the server copies a PostgreSQL database only while no other session
      // is connected to it: close the test's pooled connections, and poll on short ones.
      HikariDataSource pool = (HikariDataSource) db.dataSource();
      pool.setMinimumIdle(0);
      pool.getHikariPoolMXBean().softEvictConnections();
      Process server =
          new ProcessBuilder(command)
              .directory(new File(System.getProperty("user.dir")))
              .inheritIO()
              .start();
      int filled;
      try {
        long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
        while ((filled = countOnAConnectionOfItsOwn(progress)) == 0) {
          assertThat(server.isAlive()).as("the server is running").isTrue();
          assertThat(System.nanoTime()).as("the step started in time").isLessThan(deadline);
          Thread.sleep(100);
        }
      } finally {
        server.destroy(); // SIGTERM, during the batch after the rows filled so far
        assertThat(server.waitFor(2, TimeUnit.MINUTES)).isTrue();
      }
      assertThat(server.exitValue()).isNotZero();
      return filled;
    } finally {
      Files.delete(properties);
    }
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
    return count(FILLED);
  }

  private int countOnAConnectionOfItsOwn(String sql) throws SQLException {
    HikariDataSource pool = (HikariDataSource) db.dataSource();
    try (Connection connection =
            DriverManager.getConnection(db.jdbcUrl(), pool.getUsername(), pool.getPassword());
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      rs.next();
      return rs.getInt(1);
    }
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
