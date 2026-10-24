package io.unitycatalog.server.testdatabase;

import io.unitycatalog.server.utils.ServerProperties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;

/**
 * One MySQL container per test JVM; each test gets its own database and drops it afterwards. MySQL
 * has no {@code CREATE DATABASE … TEMPLATE}, so each database's schema is built by Hibernate
 * ({@code create-only}). Testcontainers' reaper removes the container when the JVM exits.
 */
final class MySqlDatabases {

  private static final Logger LOGGER = LoggerFactory.getLogger(MySqlDatabases.class);
  private static MySqlDatabases instance;

  private final SupportedDatabase database;
  private final MySQLContainer<?> container;
  private final RuntimeException failure;

  private MySqlDatabases(
      SupportedDatabase database, MySQLContainer<?> container, RuntimeException failure) {
    this.database = database;
    this.container = container;
    this.failure = failure;
  }

  /** The container for the selected MySQL database, started on first use. */
  static synchronized MySqlDatabases get() {
    if (instance == null) {
      instance = start(SupportedDatabase.selected());
    }
    return instance;
  }

  private static MySqlDatabases start(SupportedDatabase database) {
    try {
      // The root user may create and drop databases; the default test user may not.
      MySQLContainer<?> container =
          new MySQLContainer<>(database.image().orElseThrow()).withUsername("root");
      container.start();
      LOGGER.info("Running tests on {} ({})", database, database.image().orElseThrow());
      return new MySqlDatabases(database, container, null);
    } catch (RuntimeException e) {
      return new MySqlDatabases(
          database,
          null,
          new IllegalStateException(
              database + " selected but its container could not start: " + e.getMessage(), e));
    }
  }

  /** A database with the schema Hibernate builds from the entities. */
  TestDatabase newDatabase(String name) {
    if (failure != null) {
      throw new IllegalStateException(failure.getMessage() + " (cached; not retried)", failure);
    }
    // utf8mb4_0900_bin compares text case-sensitively, like PostgreSQL and H2. Storage paths and
    // names are case-sensitive; MySQL's default collation is not. Tables take the database's.
    execute("CREATE DATABASE " + name + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin");
    String url =
        "jdbc:mysql://"
            + container.getHost()
            + ":"
            + container.getMappedPort(MySQLContainer.MYSQL_PORT)
            + "/"
            + name;
    TestDatabase testDatabase =
        new TestDatabase(
            "com.mysql.cj.jdbc.Driver",
            url,
            container.getUsername(),
            container.getPassword(),
            "none",
            () -> execute("DROP DATABASE " + name));
    try {
      buildSchema(testDatabase);
    } catch (RuntimeException e) {
      testDatabase.close();
      throw e;
    }
    return testDatabase;
  }

  private void buildSchema(TestDatabase database) {
    Properties create = database.hibernateProperties(new ServerProperties(new Properties()));
    create.setProperty("hibernate.hbm2ddl.auto", "create-only");
    // MySQL rejects the index on uc_staging_tables.staging_location (key too long); carry on
    // without it, as a real MySQL install does.
    create.setProperty("hibernate.hbm2ddl.halt_on_error", "false");
    // Closing the configurator releases its connections; the database stays.
    database.openHibernateConfigurator(create).close();
  }

  private void execute(String sql) {
    try (Connection connection =
        DriverManager.getConnection(
            container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
      connection.createStatement().execute(sql);
    } catch (SQLException e) {
      throw new IllegalStateException(sql + " failed on " + database, e);
    }
  }
}
