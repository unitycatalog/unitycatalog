package io.unitycatalog.server.persist;

import java.util.Properties;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class PostgresLifecycleSchemaMigrationTest extends AbstractLifecycleSchemaMigrationTest {
  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("unitycatalog_test")
          .withUsername("test")
          .withPassword("test");

  @Override
  protected Properties databaseProperties() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
    properties.setProperty("hibernate.connection.url", POSTGRES.getJdbcUrl());
    properties.setProperty("hibernate.connection.username", POSTGRES.getUsername());
    properties.setProperty("hibernate.connection.password", POSTGRES.getPassword());
    return properties;
  }
}
