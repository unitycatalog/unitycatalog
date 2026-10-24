package io.unitycatalog.server.migrate.teststeps.batched;

import io.unitycatalog.server.migrate.DataStep;
import io.unitycatalog.server.migrate.teststeps.UpperNames;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** A test-only step: fills each row's upper-case name, 2 rows per batch. */
public class V2001__Fill_upper_name_in_batches extends DataStep {

  private static volatile boolean stopAfterFirstBatch;

  /** When set, the second batch fails before it changes anything. */
  public static void stopAfterFirstBatch(boolean stop) {
    stopAfterFirstBatch = stop;
  }

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
                  + " WHERE upper_name IS NULL LIMIT ?";
          case H2, POSTGRESQL ->
              "UPDATE test_data_step_items SET upper_name = UPPER(name) WHERE id IN"
                  + " (SELECT id FROM test_data_step_items WHERE upper_name IS NULL LIMIT ?)";
        };
    updateInBatches(stopAfterFirstBatch ? failingSecondUpdate(connection) : connection, sql, 2);
  }

  /** The connection, but the second update of each statement it prepares fails. */
  private static Connection failingSecondUpdate(Connection connection) {
    return proxy(
        Connection.class,
        (proxy, method, args) -> {
          Object result = invoke(method, connection, args);
          if (!method.getName().equals("prepareStatement")) {
            return result;
          }
          PreparedStatement statement = (PreparedStatement) result;
          int[] updates = {0};
          return proxy(
              PreparedStatement.class,
              (p, m, a) -> {
                if (m.getName().equals("executeUpdate") && ++updates[0] == 2) {
                  throw new SQLException("Stopped by the test after the first batch");
                }
                return invoke(m, statement, a);
              });
        });
  }

  private static <T> T proxy(Class<T> type, InvocationHandler handler) {
    return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
  }

  private static Object invoke(Method method, Object target, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
