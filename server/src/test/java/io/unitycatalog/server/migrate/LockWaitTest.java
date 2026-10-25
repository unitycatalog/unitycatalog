package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Test-only steps create {@code test_data_step_items} (version 2000) and add a column to it
 * (version 2100) while another session holds a lock on it.
 */
public class LockWaitTest {

  private static final String CREATE_TABLE = "classpath:db/test-steps/data/";
  private static final String ADD_COLUMN = "classpath:db/test-steps/lock-wait/";

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void stepWaitingForALockFailsWithinTheCap() throws Exception {
    migrator(CREATE_TABLE).migrate();
    try (Connection other = db.dataSource().getConnection();
        Statement statement = other.createStatement()) {
      other.setAutoCommit(false);
      statement.executeUpdate("INSERT INTO test_data_step_items (id, name) VALUES (1, 'held')");
      long start = System.nanoTime();

      assertThatThrownBy(
              new SchemaMigrator(
                      db.dataSource(),
                      db.kind(),
                      locations(CREATE_TABLE, ADD_COLUMN),
                      Duration.ofSeconds(2))
                  ::migrate)
          .hasMessage(
              "Step 2100 (add test data step item note) waited more than 2 seconds for a lock on"
                  + " test_data_step_items; another session holds it. Stop the other sessions on"
                  + " the database, then start the server again.");
      assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(30));
      other.rollback();
    }

    // H2 and MySQL recorded the step as failed; the rerun finds its column absent and runs it.
    SchemaMigrator migrator = migrator(CREATE_TABLE, ADD_COLUMN);
    migrator.migrate();
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  private SchemaMigrator migrator(String... steps) {
    return new SchemaMigrator(db.dataSource(), db.kind(), locations(steps));
  }

  private List<String> locations(String... steps) {
    return List.of(steps).stream().map(s -> s + db.kind().folder()).toList();
  }
}
