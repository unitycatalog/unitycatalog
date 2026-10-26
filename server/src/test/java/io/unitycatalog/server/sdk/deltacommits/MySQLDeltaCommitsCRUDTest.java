package io.unitycatalog.server.sdk.deltacommits;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import java.sql.Connection;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs all Delta Commits CRUD tests against a real MySQL database via Testcontainers. Automatically
 * skipped when Docker is not available. MySQL is the dialect that constrains the batch DELETE in
 * DeltaCommitRepository: it rejects LIMIT directly inside an IN(...) subquery (error 1235) and a
 * subquery selecting from the table being deleted from (error 1093), so this suite is what guards
 * the derived-table shape of that query against regressions that H2 and PostgreSQL would not catch.
 *
 * <p>MySQL's default REPEATABLE READ also answers a plain read from the transaction's first
 * snapshot, so this is the only suite that catches a commit re-reading the locked table row without
 * a locking read.
 */
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
    // The container is shared by the class's tests; create-drop gives each test a fresh schema.
    hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    // create-drop first drops the schema, and on a fresh MySQL database dropping its foreign keys
    // fails harmlessly: MySQL has no DROP FOREIGN KEY IF EXISTS.
    hibernateProperties.setProperty("hibernate.hbm2ddl.halt_on_error", "false");
  }

  @Test
  public void testCommitThatWaitedForTheLockReadsTheOtherCommit() {
    TableInfo table =
        createTestingTable("lock_table", TableType.MANAGED, Optional.empty(), tableOperations);
    UUID tableId = UUID.fromString(table.getTableId());
    SessionFactory sessionFactory = hibernateConfigurator.getSessionFactory();
    setMetadataLocation(sessionFactory, tableId, "m0");

    try (Session commit = sessionFactory.openSession()) {
      Transaction tx = commit.beginTransaction();
      // Without REPEATABLE READ this test would pass without testing anything.
      assertThat(commit.doReturningWork(Connection::getTransactionIsolation))
          .isEqualTo(Connection.TRANSACTION_REPEATABLE_READ);
      // The first read takes this transaction's snapshot: m0.
      TableInfoDAO dao = commit.get(TableInfoDAO.class, tableId);
      assertThat(dao.getIcebergMetadataLocation()).isEqualTo("m0");
      // Another commit lands m1 before this one takes the lock.
      setMetadataLocation(sessionFactory, tableId, "m1");

      RepositoryUtils.lockTableForCommit(
          commit, dao, tableId, Optional.empty(), ErrorCode.UPDATE_REQUIREMENT_CONFLICT);

      assertThat(dao.getIcebergMetadataLocation()).isEqualTo("m1");
      tx.rollback();
    }
  }

  private static void setMetadataLocation(
      SessionFactory sessionFactory, UUID tableId, String location) {
    try (Session session = sessionFactory.openSession()) {
      Transaction tx = session.beginTransaction();
      session.get(TableInfoDAO.class, tableId).setIcebergMetadataLocation(location);
      tx.commit();
    }
  }
}
