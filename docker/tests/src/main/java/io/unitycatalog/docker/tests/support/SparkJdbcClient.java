package io.unitycatalog.docker.tests.support;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/** Run Spark SQL against the long-running Thrift Server via Hive JDBC (host → localhost:10000). */
public final class SparkJdbcClient {

  static {
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch (ClassNotFoundException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private SparkJdbcClient() {}

  private static final int QUERY_TIMEOUT_SECS =
      Integer.parseInt(System.getenv().getOrDefault("DOCKER_TESTS_SPARK_SQL_TIMEOUT_SECS", "180"));

  public static String execute(String schema, String sql) throws SQLException {
    String jdbcUrl = jdbcUrl(schema);
    System.err.println("==> Spark SQL [" + schema + "]: " + sql);
    try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
      connection.setAutoCommit(true);
      return executeSql(connection, sql);
    }
  }

  /** Wait until the Thrift listener accepts TCP connections (compose healthcheck is not enough for JDBC). */
  public static void waitForPort() throws IOException, InterruptedException {
    IOException last = null;
    for (int attempt = 0; attempt < 60; attempt++) {
      try (Socket socket = new Socket()) {
        socket.connect(
            new InetSocketAddress(
                DockerTestConfig.SPARK_JDBC_HOST, DockerTestConfig.SPARK_JDBC_PORT),
            2000);
        return;
      } catch (IOException e) {
        last = e;
        Thread.sleep(1000L);
      }
    }
    throw last != null
        ? last
        : new IOException(
            "Spark Thrift port not open on "
                + DockerTestConfig.SPARK_JDBC_HOST
                + ":"
                + DockerTestConfig.SPARK_JDBC_PORT);
  }

  private static String jdbcUrl(String schema) {
    return String.format(
        "jdbc:hive2://%s:%d/%s",
        DockerTestConfig.SPARK_JDBC_HOST, DockerTestConfig.SPARK_JDBC_PORT, schema);
  }

  private static String executeSql(Connection connection, String sql) throws SQLException {
    String trimmed = sql.strip();
    if (trimmed.isEmpty()) {
      return "";
    }
    StringBuilder output = new StringBuilder();
    try (Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(QUERY_TIMEOUT_SECS);
      boolean hasResultSet = statement.execute(trimmed);
      if (hasResultSet) {
        appendResultSet(output, statement.getResultSet());
      }
      while (statement.getMoreResults() || statement.getUpdateCount() != -1) {
        if (statement.getResultSet() != null) {
          appendResultSet(output, statement.getResultSet());
        }
      }
    }
    return output.toString();
  }

  private static void appendResultSet(StringBuilder output, ResultSet resultSet)
      throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    int columnCount = metadata.getColumnCount();
    while (resultSet.next()) {
      for (int column = 1; column <= columnCount; column++) {
        if (column > 1) {
          output.append('\t');
        }
        Object value = resultSet.getObject(column);
        output.append(value == null ? "null" : value.toString());
      }
      output.append('\n');
    }
  }
}
