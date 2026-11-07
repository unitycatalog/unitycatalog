package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.utils.ServerProperties;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;
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
