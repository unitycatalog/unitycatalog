package io.unitycatalog.server.persist;

import java.util.Properties;

class H2LifecycleSchemaMigrationTest extends AbstractLifecycleSchemaMigrationTest {
  @Override
  protected Properties databaseProperties() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    properties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:lifecycle_migration;DB_CLOSE_DELAY=-1");
    return properties;
  }
}
