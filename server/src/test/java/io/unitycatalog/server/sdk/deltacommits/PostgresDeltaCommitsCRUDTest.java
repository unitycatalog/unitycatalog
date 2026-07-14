package io.unitycatalog.server.sdk.deltacommits;

import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs all Delta Commits CRUD tests against a real PostgreSQL database via Testcontainers.
 * Automatically skipped when Docker is not available. The container is started by the
 * {@code @Testcontainers} extension before any test lifecycle method runs; the connection settings
 * are injected per-fixture through {@link #setUpHibernateProperties(Properties)} so they never leak
 * to other test classes.
 */
@Tag("database")
@Testcontainers(disabledWithoutDocker = true)
public class PostgresDeltaCommitsCRUDTest extends SdkDeltaCommitsCRUDTest {

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("unitycatalog_test")
          .withUsername("test")
          .withPassword("test");

  @Override
  protected void setUpHibernateProperties(Properties hibernateProperties) {
    hibernateProperties.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
    hibernateProperties.setProperty("hibernate.connection.url", POSTGRES.getJdbcUrl());
    hibernateProperties.setProperty("hibernate.connection.username", POSTGRES.getUsername());
    hibernateProperties.setProperty("hibernate.connection.password", POSTGRES.getPassword());
  }
}
