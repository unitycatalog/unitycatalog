package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
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
 * Checks on MySQL that a commit waiting for the table lock then reads the latest row. MySQL's
 * default REPEATABLE READ answers a plain read from the transaction's first snapshot, so H2 and
 * PostgreSQL (READ COMMITTED) cannot catch a re-read that is not a locking read. Skipped when
 * Docker is not available.
 */
@Testcontainers(disabledWithoutDocker = true)
public class MySQLTableLockTest extends BaseTableCRUDTestEnv {

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

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig config) {
    return new SdkTableOperations(TestUtils.createApiClient(config));
  }

  @Test
  public void commitThatWaitedForTheLockReadsTheOtherCommit() {
    TableInfo table =
        createTestingTable(
            TestUtils.TABLE_NAME, TableType.MANAGED, Optional.empty(), tableOperations);
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
