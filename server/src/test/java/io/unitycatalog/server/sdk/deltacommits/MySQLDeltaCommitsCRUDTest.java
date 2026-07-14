package io.unitycatalog.server.sdk.deltacommits;

import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs all Delta Commits CRUD tests against a real MySQL database via Testcontainers. Automatically
 * skipped when Docker is not available. MySQL is the dialect that constrains the batch DELETE in
 * DeltaCommitRepository: it rejects LIMIT directly inside an IN(...) subquery (error 1235) and a
 * subquery selecting from the table being deleted from (error 1093), so this suite is what guards
 * the derived-table shape of that query against regressions that H2 and PostgreSQL would not catch.
 */
@Tag("database")
@Testcontainers(disabledWithoutDocker = true)
public class MySQLDeltaCommitsCRUDTest extends SdkDeltaCommitsCRUDTest {

  @Container
  private static final MySQLContainer<?> MYSQL =
      new MySQLContainer<>("mysql:8.4")
          .withDatabaseName("unitycatalog_test")
          .withUsername("test")
          .withPassword("test");

  @Override
  protected void setUpHibernateProperties(Properties hibernateProperties) {
    hibernateProperties.setProperty(
        "hibernate.connection.driver_class", "com.mysql.cj.jdbc.Driver");
    hibernateProperties.setProperty("hibernate.connection.url", MYSQL.getJdbcUrl());
    hibernateProperties.setProperty("hibernate.connection.username", MYSQL.getUsername());
    hibernateProperties.setProperty("hibernate.connection.password", MYSQL.getPassword());
  }
}
