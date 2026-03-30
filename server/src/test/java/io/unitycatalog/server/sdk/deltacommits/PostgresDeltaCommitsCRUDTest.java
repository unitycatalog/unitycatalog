package io.unitycatalog.server.sdk.deltacommits;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration test that runs all Delta Commits CRUD tests against a real PostgreSQL database via
 * Testcontainers. This verifies cross-database compatibility of SQL queries, particularly the
 * subquery-based DELETE pattern used in DeltaCommitRepository (issue #1385).
 *
 * <p>To run: {@code build/sbt "server/testOnly *PostgresDeltaCommitsCRUDTest"}
 *
 * <p>Requires Docker to be running. Excluded from default test runs via {@code @Tag("postgres")}.
 */
@Tag("postgres")
@Testcontainers
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
