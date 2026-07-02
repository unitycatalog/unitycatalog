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
import java.util.Properties;
import java.util.LinkedHashMap;
import java.util.Map;

/** Run Spark SQL against the long-running Thrift Server via Hive JDBC (host → localhost:10000). */
public final class SparkJdbcClient {

  private static final String UC_CATALOG_CLASS = "io.unitycatalog.spark.UCSingleCatalog";

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

  public static String execute(String catalog, String token, String schema, String sql)
      throws SQLException {
    String jdbcUrl = jdbcUrl(schema);
    Properties sessionConf = sessionConfProperties(catalog, token);
    System.err.println(
        "==> Spark SQL [" + catalog + "." + schema + "] via " + jdbcUrl + ": " + sql);
    try (Connection connection = DriverManager.getConnection(jdbcUrl, sessionConf)) {
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

  static String jdbcUrl(String schema) {
    return String.format(
        "jdbc:hive2://%s:%d/%s",
        DockerTestConfig.SPARK_JDBC_HOST, DockerTestConfig.SPARK_JDBC_PORT, schema);
  }

  private static Properties sessionConfProperties(String catalog, String token) {
    Properties properties = new Properties();
    for (Map.Entry<String, String> entry : catalogSessionConf(catalog, token).entrySet()) {
      properties.setProperty("hiveconf:" + entry.getKey(), entry.getValue());
    }
    return properties;
  }

  private static Map<String, String> catalogSessionConf(String catalog, String token) {
    Map<String, String> conf = new LinkedHashMap<>();
    conf.put("spark.sql.catalog." + catalog, UC_CATALOG_CLASS);
    conf.put("spark.sql.catalog." + catalog + ".uri", DockerTestConfig.SPARK_UC_SERVER_URI);
    conf.put("spark.sql.catalog." + catalog + ".token", token);
    conf.put("spark.sql.catalog." + catalog + ".warehouse", catalog);
    conf.put("spark.sql.defaultCatalog", catalog);
    return conf;
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
