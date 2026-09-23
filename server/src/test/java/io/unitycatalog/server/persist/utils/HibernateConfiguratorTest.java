package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class HibernateConfiguratorTest {

  @Test
  void connectionPoolSizeUsesConfiguredValue() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.pool_size", "3");

    assertThat(HibernateConfigurator.connectionPoolSize(properties)).isEqualTo(3);
  }

  @Test
  void connectionPoolSizeDefaultsToTwenty() {
    assertThat(HibernateConfigurator.connectionPoolSize(new Properties())).isEqualTo(20);
  }
}
