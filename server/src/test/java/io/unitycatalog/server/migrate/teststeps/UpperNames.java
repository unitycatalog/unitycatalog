package io.unitycatalog.server.migrate.teststeps;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** The probe of the test-only data steps, which fill {@code test_data_step_items.upper_name}. */
public final class UpperNames {

  private UpperNames() {}

  /** Whether no row is left without its upper-case name. */
  public static boolean allFilled(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                "SELECT COUNT(*) FROM test_data_step_items WHERE upper_name IS NULL")) {
      rs.next();
      return rs.getInt(1) == 0;
    }
  }
}
