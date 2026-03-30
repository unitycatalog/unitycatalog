package io.unitycatalog.server.sdk.deltacommits;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs all Delta Commits CRUD tests against a real PostgreSQL database via Testcontainers.
 * Automatically skipped when Docker is not available.
 */
@Testcontainers(disabledWithoutDocker = true)
public class PostgresDeltaCommitsCRUDTest extends SdkDeltaCommitsCRUDTest {

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("unitycatalog_test")
          .withUsername("test")
          .withPassword("test");

  @BeforeAll
  static void setUpPostgres() {
    POSTGRES.start();
    System.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
    System.setProperty("hibernate.connection.url", POSTGRES.getJdbcUrl());
    System.setProperty("hibernate.connection.username", POSTGRES.getUsername());
    System.setProperty("hibernate.connection.password", POSTGRES.getPassword());
  }

  @AfterAll
  static void tearDownPostgres() {
    System.clearProperty("hibernate.connection.driver_class");
    System.clearProperty("hibernate.connection.url");
    System.clearProperty("hibernate.connection.username");
    System.clearProperty("hibernate.connection.password");
  }
}
