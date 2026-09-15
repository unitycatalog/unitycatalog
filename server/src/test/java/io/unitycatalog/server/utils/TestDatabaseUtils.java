package io.unitycatalog.server.utils;

import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

/** Configures the metadata database used by server tests. */
public final class TestDatabaseUtils {

  static final String TEST_DATABASE_ENV = "UC_TEST_DATABASE";

  private static final DatabaseType SELECTED_DATABASE =
      parseDatabaseType(System.getenv(TEST_DATABASE_ENV));
  private static JdbcDatabaseContainer<?> databaseContainer;
  private static boolean schemaCreated;

  private TestDatabaseUtils() {}

  /** Uses H2 by default and starts a shared Testcontainers database when explicitly requested. */
  public static synchronized void configureHibernateProperties(Properties hibernateProperties) {
    if (SELECTED_DATABASE == DatabaseType.H2) {
      return;
    }

    JdbcDatabaseContainer<?> container = getOrStartContainer();
    if (schemaCreated) {
      clearDatabase(container);
    }
    hibernateProperties.setProperty(
        "hibernate.connection.driver_class", container.getDriverClassName());
    hibernateProperties.setProperty("hibernate.connection.url", container.getJdbcUrl());
    hibernateProperties.setProperty("hibernate.connection.username", container.getUsername());
    hibernateProperties.setProperty("hibernate.connection.password", container.getPassword());
    hibernateProperties.setProperty(
        "hibernate.hbm2ddl.auto", schemaCreated ? "none" : "create-only");
    schemaCreated = true;
  }

  /** Creates a configurator for tests that use Hibernate without {@code BaseServerTest}. */
  public static HibernateConfigurator createHibernateConfigurator() {
    Properties properties = new Properties();
    properties.setProperty(Property.SERVER_ENV.getKey(), "test");
    Properties hibernateProperties =
        HibernateConfigurator.setupHibernateProperties(new ServerProperties(properties));
    configureHibernateProperties(hibernateProperties);
    return new HibernateConfigurator(hibernateProperties);
  }

  static DatabaseType parseDatabaseType(String value) {
    if (value == null || value.isBlank()) {
      return DatabaseType.H2;
    }

    try {
      return DatabaseType.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          TEST_DATABASE_ENV + " must be one of: h2, postgresql, mysql; found: " + value, e);
    }
  }

  private static JdbcDatabaseContainer<?> getOrStartContainer() {
    if (databaseContainer == null) {
      databaseContainer = createContainer(SELECTED_DATABASE);
      databaseContainer.start();
    }
    return databaseContainer;
  }

  private static void clearDatabase(JdbcDatabaseContainer<?> container) {
    try (Connection connection =
        DriverManager.getConnection(
            container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
      switch (SELECTED_DATABASE) {
        case POSTGRESQL -> clearPostgres(connection);
        case MYSQL -> clearMySql(connection);
        case H2 -> throw new IllegalArgumentException("H2 does not use a container");
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to reset test database", e);
    }
  }

  private static void clearPostgres(Connection connection) throws SQLException {
    List<String> tables = new ArrayList<>();
    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")) {
        while (resultSet.next()) {
          tables.add('"' + resultSet.getString(1).replace("\"", "\"\"") + '"');
        }
      }
      if (!tables.isEmpty()) {
        statement.execute("TRUNCATE TABLE " + String.join(", ", tables) + " CASCADE");
      }
    }
  }

  private static void clearMySql(Connection connection) throws SQLException {
    List<String> tables = new ArrayList<>();
    try (Statement statement = connection.createStatement()) {
      statement.execute("SET FOREIGN_KEY_CHECKS = 0");
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT table_name FROM information_schema.tables "
                    + "WHERE table_schema = DATABASE()")) {
          while (resultSet.next()) {
            tables.add(resultSet.getString(1));
          }
        }
        for (String table : tables) {
          statement.execute("TRUNCATE TABLE `" + table.replace("`", "``") + "`");
        }
      } finally {
        statement.execute("SET FOREIGN_KEY_CHECKS = 1");
      }
    }
  }

  private static JdbcDatabaseContainer<?> createContainer(DatabaseType databaseType) {
    return switch (databaseType) {
      case POSTGRESQL ->
          new PostgreSQLContainer<>("postgres:16-alpine")
              .withDatabaseName("unitycatalog_test")
              .withUsername("test")
              .withPassword("test");
      case MYSQL ->
          new MySQLContainer<>("mysql:8.4")
              .withDatabaseName("unitycatalog_test")
              .withUsername("test")
              .withPassword("test");
      case H2 -> throw new IllegalArgumentException("H2 does not use a container");
    };
  }

  enum DatabaseType {
    H2,
    POSTGRESQL,
    MYSQL
  }
}
