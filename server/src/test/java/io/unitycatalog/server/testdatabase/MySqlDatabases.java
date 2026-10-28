package io.unitycatalog.server.testdatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

/**
 * MySQL test databases. MySQL has no {@code CREATE DATABASE … TEMPLATE}, so each database replays
 * the {@code SHOW CREATE TABLE} statements captured from {@code uc_template}, then copies the
 * template's rows (Flyway's history), as PostgreSQL's TEMPLATE does.
 */
final class MySqlDatabases extends TemplateDatabases {

  private static MySqlDatabases instance;

  private List<String> templateTables;
  private List<String> templateDdl;

  private MySqlDatabases(SupportedDatabase database) {
    super(database);
  }

  /** The container for the selected MySQL database, started on first use. */
  static synchronized MySqlDatabases get() {
    if (instance == null) {
      MySqlDatabases created = new MySqlDatabases(SupportedDatabase.selected());
      created.start();
      instance = created;
    }
    return instance;
  }

  @Override
  JdbcDatabaseContainer<?> newContainer(String image) {
    // The root user may create and drop databases; the default test user may not.
    return new MySQLContainer<>(image)
        .withUsername("root")
        // Test-only speed settings (data is thrown away):
        .withCommand(
            // Flush the redo log about once a second instead of at every commit.
            "--innodb-flush-log-at-trx-commit=0",
            // No binary log (no replication or point-in-time recovery needed).
            "--skip-log-bin",
            // No doublewrite buffer (it only guards against torn pages on a crash).
            "--innodb-doublewrite=OFF");
  }

  @Override
  String dataDirectory() {
    return "/var/lib/mysql";
  }

  @Override
  String driver() {
    return "com.mysql.cj.jdbc.Driver";
  }

  @Override
  String jdbcUrl(String name) {
    return "jdbc:mysql://"
        + container.getHost()
        + ":"
        + container.getMappedPort(MySQLContainer.MYSQL_PORT)
        + "/"
        + name;
  }

  @Override
  void createDatabase(String name) {
    // utf8mb4_0900_bin compares text exactly, like PostgreSQL and H2. Storage paths compare
    // exactly; MySQL's default collation ignores case. Tables take the database's.
    execute("CREATE DATABASE " + name + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin");
  }

  @Override
  void templateBuilt() {
    templateTables = new ArrayList<>();
    templateDdl = new ArrayList<>();
    try (Connection connection = connect(TEMPLATE);
        Statement statement = connection.createStatement()) {
      try (ResultSet rs = statement.executeQuery("SHOW TABLES")) {
        while (rs.next()) {
          templateTables.add(rs.getString(1));
        }
      }
      for (String table : templateTables) {
        try (ResultSet rs = statement.executeQuery("SHOW CREATE TABLE `" + table + "`")) {
          rs.next();
          templateDdl.add(rs.getString(2));
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Could not read the template schema of " + database, e);
    }
  }

  @Override
  void cloneTemplate(String name) {
    createDatabase(name);
    try (Connection connection = connect(name);
        Statement statement = connection.createStatement()) {
      // Foreign keys may point at tables created later.
      statement.addBatch("SET FOREIGN_KEY_CHECKS=0");
      for (String ddl : templateDdl) {
        statement.addBatch(ddl);
      }
      for (String table : templateTables) {
        statement.addBatch(
            "INSERT INTO `" + table + "` SELECT * FROM " + TEMPLATE + ".`" + table + "`");
      }
      statement.executeBatch();
    } catch (SQLException e) {
      // newDatabase has no TestDatabase to close yet, so drop the half-built database here.
      IllegalStateException failure =
          new IllegalStateException("Could not clone the template into " + name, e);
      try {
        execute(dropSql(name));
      } catch (RuntimeException dropFailure) {
        failure.addSuppressed(dropFailure);
      }
      throw failure;
    }
  }

  @Override
  String dropSql(String name) {
    return "DROP DATABASE " + name;
  }

  private Connection connect(String name) throws SQLException {
    return DriverManager.getConnection(
        jdbcUrl(name), container.getUsername(), container.getPassword());
  }
}
