package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class HibernateConfiguratorTest {

  @Test
  void configuresHikariAndExposesTheSharedDataSource() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    properties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    properties.setProperty("hibernate.connection.user", "sa");
    properties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
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
}
