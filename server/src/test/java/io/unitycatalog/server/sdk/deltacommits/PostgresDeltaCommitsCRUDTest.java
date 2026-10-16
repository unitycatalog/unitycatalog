package io.unitycatalog.server.sdk.deltacommits;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.testdatabase.UsesOwnDatabase;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;
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
@UsesOwnDatabase(reason = "PostgreSQL coverage until the whole suite runs on PostgreSQL")
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

  /**
   * PostgreSQL limits a btree index entry to 2,704 bytes. A longer staging location fails the
   * insert into idx_staging_location, which the request caused, so it is INVALID_ARGUMENT.
   */
  @Test
  public void testValueOverIndexLimitIsInvalidArgument() {
    StagingTableDAO stagingTableDAO = new StagingTableDAO();
    stagingTableDAO.setDefaultFields();
    stagingTableDAO.setId(UUID.randomUUID());
    stagingTableDAO.setName("staging_table");
    // 1,917 characters and 3,817 bytes: fits varchar(2048) but not an index entry.
    stagingTableDAO.setStagingLocation("s3://test-bucket/" + randomTwoByteCharacters(1900));

    assertThatThrownBy(
            () ->
                TransactionManager.executeWithTransaction(
                    hibernateConfigurator.getSessionFactory(),
                    session -> {
                      session.persist(stagingTableDAO);
                      return null;
                    },
                    "Error creating staging table",
                    /* readOnly= */ false))
        .isInstanceOf(BaseException.class)
        .hasFieldOrPropertyWithValue("errorCode", ErrorCode.INVALID_ARGUMENT)
        .hasMessageContaining("index row size");
  }

  /**
   * Returns random letters that each take 2 bytes in UTF-8. They are random because PostgreSQL
   * compresses an index entry before it checks the size.
   */
  private static String randomTwoByteCharacters(int count) {
    // Latin Extended-A, Latin Extended-B and IPA Extensions: U+0100 to U+02AF.
    Random random = new Random(/* seed= */ 0);
    StringBuilder characters = new StringBuilder();
    for (int i = 0; i < count; i++) {
      characters.append((char) (0x0100 + random.nextInt(0x02B0 - 0x0100)));
    }
    return characters.toString();
  }
}
