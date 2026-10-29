package io.unitycatalog.server.testdatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

/**
 * MySQL test databases. MySQL has no {@code CREATE DATABASE … TEMPLATE}, so each database replays
 * the {@code SHOW CREATE TABLE} statements captured from {@code uc_template}.
 */
final class MySqlDatabases extends TemplateDatabases {

  private static MySqlDatabases instance;

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
            "--innodb-doublewrite=OFF",
            // PostgreSQL drops a database WITH (FORCE). MySQL's DROP DATABASE waits for locks held
            // by open transactions, for a year by default; fail a test that leaks one instead.
            "--lock-wait-timeout=60");
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
  void configureTemplate(Properties hibernateProperties) {
    // Hibernate's DDL always includes one index MySQL rejects: staging_location is 2048
    // characters, 8192 bytes in utf8mb4, over InnoDB's 3072-byte key limit. With halt_on_error the
    // template build would fail on every run, so carry on without that index, as the server does
    // by default (it leaves halt_on_error at Hibernate's default, false). templateBuilt then checks
    // that every table was created. TODO: remove once the template is built by the schema
    // migration steps (#2045), which leave this index out and stop on any error.
    hibernateProperties.setProperty("hibernate.hbm2ddl.halt_on_error", "false");
  }

  @Override
  void templateBuilt(List<String> mappedTables) {
    templateDdl = new ArrayList<>();
    try (Connection connection = connect(TEMPLATE);
        Statement statement = connection.createStatement()) {
      List<String> tables = new ArrayList<>();
      try (ResultSet rs = statement.executeQuery("SHOW TABLES")) {
        while (rs.next()) {
          tables.add(rs.getString(1));
        }
      }
      // halt_on_error=false hides any failed DDL, so check every mapped table was created.
      List<String> missing = new ArrayList<>(mappedTables);
      missing.removeAll(tables);
      if (!missing.isEmpty()) {
        throw new IllegalStateException("The MySQL template schema lacks tables " + missing);
      }
      for (String table : tables) {
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
