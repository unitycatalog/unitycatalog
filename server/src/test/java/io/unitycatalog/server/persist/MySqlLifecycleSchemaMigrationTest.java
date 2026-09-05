package io.unitycatalog.server.persist;

import java.util.Properties;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class MySqlLifecycleSchemaMigrationTest extends AbstractLifecycleSchemaMigrationTest {
  @Container
  private final MySQLContainer<?> mysql =
      new MySQLContainer<>("mysql:8.4")
          .withDatabaseName("unitycatalog_test")
          .withUsername("test")
          .withPassword("test");

  @Override
  protected Properties databaseProperties() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.driver_class", "com.mysql.cj.jdbc.Driver");
    properties.setProperty("hibernate.connection.url", mysql.getJdbcUrl());
    properties.setProperty("hibernate.connection.username", mysql.getUsername());
    properties.setProperty("hibernate.connection.password", mysql.getPassword());
    return properties;
  }
}
