package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.base.BaseServerTest;
import org.junit.jupiter.api.Test;

/** BaseServerTest runs every test on its own database from {@link TestDatabases}. */
class BaseServerTestDatabaseTest extends BaseServerTest {

  @Test
  void serverUsesTheTestDatabase() {
    assertThat(testDatabase).isNotNull();
    assertThat(
            hibernateConfigurator.getHibernateProperties().getProperty("hibernate.connection.url"))
        .isEqualTo(testDatabase.jdbcUrl());
  }
}
