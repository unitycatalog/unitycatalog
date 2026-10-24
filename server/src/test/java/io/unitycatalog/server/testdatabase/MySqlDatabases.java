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
      instance = new MySqlDatabases(SupportedDatabase.selected());
      instance.start();
    }
    return instance;
  }

  @Override
  JdbcDatabaseContainer<?> newContainer(String image) {
    // The root user may create and drop databases; the default test user may not.
    return new MySQLContainer<>(image)
        .withUsername("root")
        .withCommand(
            "--innodb-flush-log-at-trx-commit=0", "--skip-log-bin", "--innodb-doublewrite=OFF");
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
    // utf8mb4_0900_bin compares text case-sensitively, like PostgreSQL and H2. Storage paths and
    // names are case-sensitive; MySQL's default collation is not. Tables take the database's.
    execute("CREATE DATABASE " + name + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin");
  }

  @Override
  void configureTemplate(Properties hibernateProperties) {
    // MySQL rejects the index on uc_staging_tables.staging_location (key too long); carry on
    // without it, as a real MySQL install does.
    hibernateProperties.setProperty("hibernate.hbm2ddl.halt_on_error", "false");
  }

  @Override
  void templateBuilt() {
    templateDdl = new ArrayList<>();
    try (Connection connection = connect(TEMPLATE);
        Statement statement = connection.createStatement()) {
      List<String> tables = new ArrayList<>();
      try (ResultSet rs = statement.executeQuery("SHOW TABLES")) {
        while (rs.next()) {
          tables.add(rs.getString(1));
        }
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
      templateDdl.forEach(
          ddl -> {
            try {
              statement.addBatch(ddl);
            } catch (SQLException e) {
              throw new IllegalStateException(e);
            }
          });
      statement.executeBatch();
    } catch (SQLException e) {
      throw new IllegalStateException("Could not clone the template into " + name, e);
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
