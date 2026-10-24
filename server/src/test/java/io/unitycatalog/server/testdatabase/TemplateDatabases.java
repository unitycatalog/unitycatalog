package io.unitycatalog.server.testdatabase;

import io.unitycatalog.server.utils.ServerProperties;
import jakarta.persistence.Table;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * One database container per test JVM. Hibernate builds the schema once into {@code uc_template};
 * each test gets a clone of it and drops it afterwards. Testcontainers' reaper removes the
 * container when the JVM exits. Subclasses supply what differs between databases: the container and
 * its data directory, the driver and JDBC URL, how to create, clone and drop a database, and
 * optional hooks around the template build.
 */
abstract class TemplateDatabases {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemplateDatabases.class);
  static final String TEMPLATE = "uc_template";

  final SupportedDatabase database;
  JdbcDatabaseContainer<?> container;
  private RuntimeException failure;

  TemplateDatabases(SupportedDatabase database) {
    this.database = database;
  }

  /**
   * A container for {@code image} with this database's test-only speed settings; {@link #start}
   * mounts {@link #dataDirectory} in memory.
   */
  abstract JdbcDatabaseContainer<?> newContainer(String image);

  /** The container's data directory; kept in memory, since tests need no durability. */
  abstract String dataDirectory();

  abstract String driver();

  abstract String jdbcUrl(String name);

  /** Creates the empty database {@code name}. */
  abstract void createDatabase(String name);

  /** Creates the database {@code name} with the template's schema. */
  abstract void cloneTemplate(String name);

  abstract String dropSql(String name);

  /** Adjusts how Hibernate builds the template schema. */
  void configureTemplate(Properties hibernateProperties) {}

  /** Called once the template schema exists; {@code mappedTables} are the tables Hibernate maps. */
  void templateBuilt(List<String> mappedTables) {}

  /** Starts the container and builds the template. A failure is kept and reported by each use. */
  final void start() {
    try {
      container = newContainer(database.image().orElseThrow());
      // Test-only speed settings: no durability needed.
      container.withTmpFs(Map.of(dataDirectory(), "rw"));
      container.start();
    } catch (RuntimeException e) {
      failure =
          new IllegalStateException(
              database + " selected but its container could not start: " + e.getMessage(), e);
      return;
    }
    try {
      buildTemplate();
    } catch (RuntimeException e) {
      failure = new IllegalStateException(database + " schema build failed", e);
      return;
    }
    LOGGER.info("Running tests on {} ({})", database, database.image().orElseThrow());
  }

  private void buildTemplate() {
    createDatabase(TEMPLATE);
    // Closing releases every connection: PostgreSQL refuses to copy a template that is in use.
    List<String> mappedTables;
    try (TestDatabase template = testDatabase(TEMPLATE, "create-only", () -> {})) {
      Properties create = template.hibernateProperties(new ServerProperties(new Properties()));
      configureTemplate(create);
      mappedTables =
          template
              .openHibernateConfigurator(create)
              .getSessionFactory()
              .getMetamodel()
              .getEntities()
              .stream()
              .map(entity -> entity.getJavaType().getAnnotation(Table.class).name())
              .toList();
    }
    templateBuilt(mappedTables);
  }

  final TestDatabase newDatabase(String name) {
    if (failure != null) {
      throw new IllegalStateException(failure.getMessage() + " (cached; not retried)", failure);
    }
    cloneTemplate(name);
    return testDatabase(name, "none", () -> execute(dropSql(name)));
  }

  private TestDatabase testDatabase(String name, String hbm2ddlAuto, Runnable release) {
    return new TestDatabase(
        driver(),
        jdbcUrl(name),
        container.getUsername(),
        container.getPassword(),
        hbm2ddlAuto,
        release);
  }

  final void execute(String sql) {
    try (Connection connection =
        DriverManager.getConnection(
            container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
      connection.createStatement().execute(sql);
    } catch (SQLException e) {
      throw new IllegalStateException(sql + " failed on " + database, e);
    }
  }
}
