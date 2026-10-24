package io.unitycatalog.server.migrate.teststeps.onetx;

import io.unitycatalog.server.migrate.DataStep;
import io.unitycatalog.server.migrate.teststeps.UpperNames;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** A test-only step: fills each row's upper-case name in one transaction. */
public class V2001__Fill_upper_name extends DataStep {

  @Override
  protected boolean alreadyDone(Connection connection) throws SQLException {
    return UpperNames.allFilled(connection);
  }

  @Override
  protected void apply(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          "UPDATE test_data_step_items SET upper_name = UPPER(name) WHERE upper_name IS NULL");
    }
  }
}
