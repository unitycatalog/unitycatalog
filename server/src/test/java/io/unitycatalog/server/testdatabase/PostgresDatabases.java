package io.unitycatalog.server.testdatabase;

import io.unitycatalog.server.utils.ServerProperties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One PostgreSQL container per test JVM. The schema is built once into {@code uc_template}; each
 * test gets {@code CREATE DATABASE … TEMPLATE uc_template} and drops it afterwards. Testcontainers'
 * reaper removes the container when the JVM exits.
 */
final class PostgresDatabases {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDatabases.class);
  private static final String TEMPLATE = "uc_template";
  private static PostgresDatabases instance;

  private final SupportedDatabase database;
  private final PostgreSQLContainer<?> container;
  private final RuntimeException failure;

  private PostgresDatabases(
      SupportedDatabase database, PostgreSQLContainer<?> container, RuntimeException failure) {
    this.database = database;
    this.container = container;
    this.failure = failure;
  }

  static synchronized PostgresDatabases get(SupportedDatabase database) {
    if (instance == null) {
      instance = start(database);
    }
    return instance;
  }

  private static PostgresDatabases failed(SupportedDatabase database, RuntimeException failure) {
    return new PostgresDatabases(database, null, failure);
  }

  private static PostgresDatabases start(SupportedDatabase database) {
    PostgreSQLContainer<?> container;
    try {
      container =
          new PostgreSQLContainer<>(
              DockerImageName.parse(database.image().orElseThrow())
                  .asCompatibleSubstituteFor("postgres"));
      container.start();
    } catch (RuntimeException e) {
      return failed(
          database,
          new IllegalStateException(
              database + " selected but its container could not start: " + e.getMessage(), e));
    }
    PostgresDatabases databases = new PostgresDatabases(database, container, null);
    try {
      databases.buildTemplate();
    } catch (RuntimeException e) {
      return failed(database, new IllegalStateException(database + " schema build failed", e));
    }
    LOGGER.info("Running tests on {} ({})", database, database.image().orElseThrow());
    return databases;
  }

  private void buildTemplate() {
    execute("CREATE DATABASE " + TEMPLATE);
    // Closing releases every connection: PostgreSQL refuses to copy a template that is in use.
    try (TestDatabase template = testDatabase(TEMPLATE, "create-only", () -> {})) {
      template.openHibernateConfigurator(new ServerProperties(new Properties()));
    }
  }

  TestDatabase newDatabase(String name) {
    if (failure != null) {
      throw new IllegalStateException(failure.getMessage() + " (cached; not retried)", failure);
    }
    execute("CREATE DATABASE " + name + " TEMPLATE " + TEMPLATE);
    return testDatabase(name, "none", () -> execute("DROP DATABASE " + name + " WITH (FORCE)"));
  }

  private TestDatabase testDatabase(String name, String hbm2ddlAuto, Runnable release) {
    String url =
        "jdbc:postgresql://"
            + container.getHost()
            + ":"
            + container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)
            + "/"
            + name;
    return new TestDatabase(
        "org.postgresql.Driver",
        url,
        container.getUsername(),
        container.getPassword(),
        hbm2ddlAuto,
        release);
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
