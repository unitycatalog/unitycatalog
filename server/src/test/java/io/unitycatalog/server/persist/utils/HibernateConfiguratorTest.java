package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.utils.ServerProperties;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HibernateConfiguratorTest {

  @Test
  void configuresHikariAndExposesTheSharedDataSource() {
    Properties properties = h2Properties();
    properties.setProperty("hibernate.hikari.maximumPoolSize", "3");
    properties.setProperty("hibernate.hikari.minimumIdle", "1");
    properties.setProperty("hibernate.hikari.poolName", "unity-catalog-test");

    try (HibernateConfigurator configurator = new HibernateConfigurator(properties)) {
      assertThat(configurator.getDataSource().getMaximumPoolSize()).isEqualTo(3);
      assertThat(configurator.getDataSource().getMinimumIdle()).isEqualTo(1);
      assertThat(configurator.getDataSource().getPoolName()).isEqualTo("unity-catalog-test");
      assertThat(configurator.getDataSource().isAutoCommit()).isFalse();
      assertThat(configurator.getDataSource().getUsername()).isEqualTo("sa");
      assertThat(configurator.getSessionFactory().isOpen()).isTrue();
    }
  }

  @Test
  void connectionPoolSizeFallsBackToHibernatePoolSize() {
    Properties properties = h2Properties();
    properties.setProperty("hibernate.connection.pool_size", "4");

    try (HibernateConfigurator configurator = new HibernateConfigurator(properties)) {
      assertThat(configurator.getDataSource().getMaximumPoolSize()).isEqualTo(4);
    }
  }

  @Test
  void connectionPoolSizeDefaultsToTwenty() {
    try (HibernateConfigurator configurator = new HibernateConfigurator(h2Properties())) {
      assertThat(configurator.getDataSource().getMaximumPoolSize()).isEqualTo(20);
    }
  }

  @Test
  void resolvesUsernameFromStandardHibernateProperty() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.username", "alice");
    assertThat(HibernateConfigurator.resolveConnectionUsername(properties)).isEqualTo("alice");
  }

  @Test
  void fallsBackToLegacyUserProperty() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.user", "bob");
    assertThat(HibernateConfigurator.resolveConnectionUsername(properties)).isEqualTo("bob");
  }

  @Test
  void prefersStandardUsernameWhenBothPresent() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.username", "alice");
    properties.setProperty("hibernate.connection.user", "bob");
    assertThat(HibernateConfigurator.resolveConnectionUsername(properties)).isEqualTo("alice");
  }

  @Test
  void neverLetsHibernateChangeTheSchema() {
    Properties properties = h2Properties();
    properties.setProperty("hibernate.hbm2ddl.auto", "update");

    assertThat(HibernateConfigurator.withSchemaUpdatesOff(properties))
        .containsEntry("hibernate.hbm2ddl.auto", "none");
    assertThat(
            HibernateConfigurator.setupHibernateProperties(new ServerProperties(new Properties())))
        .containsEntry("hibernate.hbm2ddl.auto", "none");
  }

  @Test
  void hibernateModeUpdatesTheSchemaWhateverTheFileSays() throws SQLException {
    Properties properties = h2Properties();
    properties.setProperty("jakarta.persistence.schema-generation.database.action", "none");
    properties.setProperty("jakarta.persistence.schema-generation.database.action.orm", "none");

    try (HibernateConfigurator configurator = new HibernateConfigurator(properties, true);
        Connection connection = configurator.getDataSource().getConnection();
        ResultSet tables =
            connection.getMetaData().getTables(null, null, "UC_CATALOGS", new String[] {"TABLE"})) {
      assertThat(tables.next()).isTrue();
    }
  }

  /**
   * Hibernate reads JVM system properties when its Environment class loads, so this runs in a JVM
   * of its own, started with a scoped schema action of none: Hibernate mode must still set up the
   * schema.
   */
  @Test
  void hibernateModeUpdatesTheSchemaWhateverTheSystemPropertiesSay(@TempDir Path dir)
      throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djakarta.persistence.schema-generation.database.action.orm=none",
                "-cp",
                System.getProperty("java.class.path"),
                ForkedHibernateMode.class.getName(),
                dir.resolve("uc").toString())
            .redirectErrorStream(true)
            .start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

    assertThat(process.waitFor()).as(output).isZero();
  }

  /** Exits with 0 when Hibernate mode on a new H2 file created the Unity Catalog tables. */
  public static final class ForkedHibernateMode {
    public static void main(String[] args) throws SQLException {
      Properties properties = new Properties();
      properties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
      properties.setProperty("hibernate.connection.url", "jdbc:h2:file:" + args[0]);
      properties.setProperty("hibernate.connection.user", "sa");
      try (HibernateConfigurator configurator = new HibernateConfigurator(properties, true);
          Connection connection = configurator.getDataSource().getConnection();
          ResultSet tables =
              connection
                  .getMetaData()
                  .getTables(null, null, "UC_CATALOGS", new String[] {"TABLE"})) {
        System.exit(tables.next() ? 0 : 1);
      }
    }
  }

  @Test
  void readsHibernateModeFromTheServerProperties() throws SQLException {
    Properties server = new Properties();
    server.setProperty("server.env", "test");
    server.setProperty("server.schema-management", "hibernate");

    try (HibernateConfigurator configurator =
            new HibernateConfigurator(new ServerProperties(server));
        Connection connection = configurator.getDataSource().getConnection()) {
      assertThat(configurator.isSchemaManagedByHibernate()).isTrue();
      // The test environment's in-memory database lasts as long as this JVM: leave it empty for
      // the next server, which sets up its schema itself.
      connection.createStatement().execute("DROP ALL OBJECTS");
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "hibernate.hbm2ddl.auto",
        "hibernate.hbm2ddl.auto.orm",
        "jakarta.persistence.schema-generation.database.action",
        "javax.persistence.schema-generation.database.action"
      })
  void refusesASchemaActionOtherThanNone(String setting) {
    Properties properties = h2Properties();
    properties.setProperty(setting, "create");

    assertThatThrownBy(() -> new HibernateConfigurator(properties))
        .hasMessageContaining(setting + "=create is not supported");
  }

  @Test
  void hibernateModeKeepsHibernatesOwnDefaultSchema() {
    Properties properties = h2Properties();
    properties.setProperty("hibernate.default_schema", "PUBLIC");

    try (HibernateConfigurator configurator = new HibernateConfigurator(properties, true)) {
      assertThat(configurator.getSessionFactory().isOpen()).isTrue();
    }
  }

  @Test
  void refusesHibernatesOwnDefaultSchema() {
    Properties properties = h2Properties();
    properties.setProperty("hibernate.default_schema", "uc");

    assertThatThrownBy(() -> new HibernateConfigurator(properties))
        .hasMessageContaining("set the schema in the JDBC URL instead");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:h2:mem:",
        "jdbc:h2:mem:;DB_CLOSE_DELAY=-1",
        "JDBC:H2:MEM:;MODE=MySQL",
        "jdbc:h2:.",
        "jdbc:h2:.;DB_CLOSE_DELAY=-1"
      })
  void refusesAnUnnamedInMemoryDatabase(String url) {
    Properties properties = h2Properties();
    properties.setProperty("hibernate.connection.url", url);

    assertThatThrownBy(() -> new HibernateConfigurator(properties))
        .hasMessageContaining("has no database name")
        .hasMessageContaining("jdbc:h2:mem:<name>;DB_CLOSE_DELAY=-1");
  }

  @Test
  void prefersThePasswordFromTheEnvironmentToTheFile() {
    Properties fileProperties = new Properties();
    fileProperties.setProperty("hibernate.connection.password", "file-password");
    HibernateConfigurator.applyPasswordOverride(
        fileProperties, null, Path.of("hibernate.properties"));
    assertThat(fileProperties.getProperty("hibernate.connection.password"))
        .isEqualTo("file-password");
    HibernateConfigurator.applyPasswordOverride(
        fileProperties, "env-password", Path.of("hibernate.properties"));
    assertThat(fileProperties.getProperty("hibernate.connection.password"))
        .isEqualTo("env-password");

    // setupHibernateProperties takes the password from the environment.
    ServerProperties withPassword =
        new ServerProperties(new Properties()) {
          @Override
          public String getEnv(String key) {
            return key.equals("hibernate.connection.password") ? "env-password" : null;
          }
        };
    assertThat(
            HibernateConfigurator.setupHibernateProperties(withPassword)
                .getProperty("hibernate.connection.password"))
        .isEqualTo("env-password");
  }

  private static Properties h2Properties() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    properties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    properties.setProperty("hibernate.connection.user", "sa");
    properties.setProperty("hibernate.hbm2ddl.auto", "none");
    return properties;
  }
}
