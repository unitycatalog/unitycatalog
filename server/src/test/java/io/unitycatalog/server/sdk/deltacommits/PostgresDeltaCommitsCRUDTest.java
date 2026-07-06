package io.unitycatalog.server.sdk.deltacommits;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs all Delta Commits CRUD tests against a real PostgreSQL database via Testcontainers.
 * Automatically skipped when Docker is not available. The container is started by the
 * {@code @Testcontainers} extension before any test lifecycle method runs; the connection overrides
 * are injected per-fixture through {@link #setUpProperties()} so they never leak to other test
 * classes.
 */
@Testcontainers(disabledWithoutDocker = true)
public class PostgresDeltaCommitsCRUDTest extends SdkDeltaCommitsCRUDTest {

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("unitycatalog_test")
          .withUsername("test")
          .withPassword("test");

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
    serverProperties.setProperty("hibernate.connection.url", POSTGRES.getJdbcUrl());
    serverProperties.setProperty("hibernate.connection.username", POSTGRES.getUsername());
    serverProperties.setProperty("hibernate.connection.password", POSTGRES.getPassword());
  }
}
