package io.unitycatalog.server.persist;

import java.util.Properties;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class MySqlLifecycleSchemaMigrationTest extends AbstractLifecycleSchemaMigrationTest {
  @Container
  private static final MySQLContainer<?> MYSQL =
      new MySQLContainer<>("mysql:8.4")
          .withDatabaseName("unitycatalog_test")
          .withUsername("test")
          .withPassword("test");

  @Override
  protected Properties databaseProperties() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.driver_class", "com.mysql.cj.jdbc.Driver");
    properties.setProperty("hibernate.connection.url", MYSQL.getJdbcUrl());
    properties.setProperty("hibernate.connection.username", MYSQL.getUsername());
    properties.setProperty("hibernate.connection.password", MYSQL.getPassword());
    return properties;
  }
}
