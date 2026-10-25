package io.unitycatalog.server.migrate.teststeps.slowbatches;

import io.unitycatalog.server.migrate.DataStep;
import io.unitycatalog.server.migrate.teststeps.UpperNames;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * A test-only step: fills each row's upper-case name, one row per batch, each batch taking a second
 * (PostgreSQL and MySQL; H2 has no sleep function).
 */
public class V2001__Fill_upper_name_slowly extends DataStep {

  @Override
  protected boolean batched() {
    return true;
  }

  @Override
  protected boolean alreadyDone(Connection connection) throws SQLException {
    return UpperNames.allFilled(connection);
  }

  @Override
  protected void apply(Connection connection) throws SQLException {
    String sql =
        switch (databaseKind(connection)) {
          case MYSQL ->
              "UPDATE test_data_step_items SET upper_name = UPPER(name)"
                  + " WHERE upper_name IS NULL AND SLEEP(1) = 0 LIMIT ?";
          case POSTGRESQL ->
              "UPDATE test_data_step_items SET upper_name = UPPER(name) WHERE id IN"
                  + " (SELECT id FROM test_data_step_items WHERE upper_name IS NULL LIMIT ?)"
                  + " AND (SELECT 1 FROM pg_sleep(1)) = 1";
          case H2 -> throw new IllegalStateException("H2 has no sleep function");
        };
    updateInBatches(connection, sql, 1);
  }
}
