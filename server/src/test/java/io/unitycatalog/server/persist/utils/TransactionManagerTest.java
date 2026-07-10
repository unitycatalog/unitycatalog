package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.DeltaCommitDAO;
import io.unitycatalog.server.utils.ServerProperties;
import java.sql.Connection;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

public class TransactionManagerTest {

  /** Tests using Mockito mocks for basic transaction contract verification. */
  @Nested
  @ExtendWith(MockitoExtension.class)
  public class MockTransactionTest {

    @Mock private SessionFactory sessionFactory;
    @Mock private Session session;
    @Mock private Transaction transaction;

    @BeforeEach
    public void setup() {
      when(sessionFactory.openSession()).thenReturn(session);
      when(session.beginTransaction()).thenReturn(transaction);
    }

    @Test
    public void testSuccessfulTransaction() {
      // Setup
      String expectedResult = "Success";

      // Execute
      String result =
          TransactionManager.executeWithTransaction(
              sessionFactory, session -> expectedResult, "Error executing operation", false);

      // Verify
      assertThat(result).isEqualTo(expectedResult);
      verify(transaction).commit();
      verify(transaction, never()).rollback();
      verify(session, never()).setDefaultReadOnly(true);
    }

    @Test
    public void testReadOnlyTransaction() {
      // Execute
      TransactionManager.executeWithTransaction(
          sessionFactory, session -> "Success", "Error executing operation", true);

      // Verify
      verify(session).setDefaultReadOnly(true);
      verify(transaction).commit();
    }

    @Test
    public void testTransactionWithBaseException() {
      // Setup
      BaseException expectedException =
          new BaseException(ErrorCode.INVALID_ARGUMENT, "Test exception");

      // Execute and verify
      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory,
                      session -> {
                        throw expectedException;
                      },
                      "Error executing operation",
                      false))
          .isEqualTo(expectedException);

      verify(transaction).rollback();
      verify(transaction, never()).commit();
    }

    @Test
    public void testTransactionWithRuntimeException() {
      // Setup
      RuntimeException testException = new RuntimeException("Test runtime exception");
      String errorMessage = "Error executing operation";

      // Execute and verify
      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory,
                      session -> {
                        throw testException;
                      },
                      errorMessage,
                      false))
          .isInstanceOf(BaseException.class)
          .hasMessageContaining(errorMessage)
          .hasMessageContaining(testException.getMessage());

      verify(transaction).rollback();
      verify(transaction, never()).commit();
    }

    @Test
    public void testSessionClosedAfterTransaction() {
      // Execute successful transaction
      TransactionManager.executeWithTransaction(
          sessionFactory, session -> "Success", "Error executing operation", false);

      // Verify session is closed
      verify(session).close();

      // Reset for error case
      reset(session);
      when(sessionFactory.openSession()).thenReturn(session);
      when(session.beginTransaction()).thenReturn(transaction);
      doThrow(new RuntimeException("Test exception")).when(transaction).commit();

      // Execute failing transaction
      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory, session -> "Success", "Error executing operation", false))
          .isInstanceOf(BaseException.class);

      // Verify session is closed even after error
      verify(session).close();
    }

    @Test
    public void testWriteInReadOnlyTransactionFails() {
      // Setup
      Object entity = new Object();
      String errorMessage = "Attempt to write in read-only session";

      // Mock a write operation failure
      RuntimeException hibernateException =
          new RuntimeException("HibernateException: cannot write in read-only session");
      doThrow(hibernateException).when(session).persist(any());
      // Execute and verify
      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory,
                      session -> {
                        session.persist(entity); // This should fail in read-only mode
                        return null;
                      },
                      errorMessage,
                      /* readOnly = */ true))
          .isInstanceOf(BaseException.class)
          .hasMessageContaining(errorMessage)
          .hasMessageContaining(hibernateException.getMessage());

      // Verify the transaction was rolled back
      verify(transaction).rollback();
      verify(transaction, never()).commit();

      // Verify session was set to read-only
      verify(session).setDefaultReadOnly(true);
    }
  }

  /**
   * Tests for the isolation level code path using a real H2 database. These verify that the
   * rollback() call before setTransactionIsolation() correctly flushes stale implicit transactions
   * from pooled connections, ensuring REPEATABLE_READ gets a fresh MVCC snapshot.
   */
  @Nested
  public class IsolationLevelWithRealDatabaseTest {

    private static SessionFactory realSessionFactory;

    @BeforeAll
    public static void setUp() {
      ServerProperties serverProperties = new ServerProperties(new Properties());
      HibernateConfigurator hibernateConfigurator = new HibernateConfigurator(serverProperties);
      realSessionFactory = hibernateConfigurator.getSessionFactory();
    }

    @AfterAll
    public static void tearDown() {
      realSessionFactory.close();
    }

    @Test
    public void testIsolationLevelIsSetAndRestored() {
      final int[] isolationDuringTx = new int[1];
      final int[] isolationAfter = new int[1];

      // Run a transaction with REPEATABLE_READ
      TransactionManager.executeWithTransaction(
          realSessionFactory,
          session -> {
            session.doWork(
                connection -> isolationDuringTx[0] = connection.getTransactionIsolation());
            return null;
          },
          "test",
          true,
          Optional.of(Connection.TRANSACTION_REPEATABLE_READ));

      // Check the isolation level was set to REPEATABLE_READ during the transaction
      assertThat(isolationDuringTx[0]).isEqualTo(Connection.TRANSACTION_REPEATABLE_READ);

      // Run another transaction without custom isolation to verify restore
      TransactionManager.executeWithTransaction(
          realSessionFactory,
          session -> {
            session.doWork(connection -> isolationAfter[0] = connection.getTransactionIsolation());
            return null;
          },
          "test",
          true);

      // Original isolation level should have been restored (H2 default is READ_COMMITTED)
      assertThat(isolationAfter[0]).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
    }

    @Test
    public void testRepeatableReadSeesLatestDataAfterRollbackFlush() {
      UUID tableId = UUID.randomUUID();

      // Step 1: Insert version 1 via a normal transaction (simulates an earlier postCommit).
      // This uses the pool and when the session closes, the connection goes back with an
      // open implicit transaction (autocommit=false).
      TransactionManager.executeWithTransaction(
          realSessionFactory,
          session -> {
            session.persist(buildCommitDAO(tableId, 1));
            return null;
          },
          "insert v1",
          false);

      // Step 2: Insert version 2 via another transaction (simulates a concurrent postCommit).
      // This may reuse the same pooled connection or a different one.
      TransactionManager.executeWithTransaction(
          realSessionFactory,
          session -> {
            session.persist(buildCommitDAO(tableId, 2));
            return null;
          },
          "insert v2",
          false);

      // Step 3: Read with REPEATABLE_READ (simulates getCommits). The rollback() in the
      // isolation level path ensures the MVCC snapshot is fresh and sees both rows.
      // Without the fix, a pooled connection with a stale implicit transaction could
      // anchor the snapshot to before version 2 was inserted.
      long maxVersion =
          TransactionManager.executeWithTransaction(
              realSessionFactory,
              session -> {
                return session
                    .createQuery(
                        "SELECT MAX(d.commitVersion) FROM DeltaCommitDAO d "
                            + "WHERE d.tableId = :tableId",
                        Long.class)
                    .setParameter("tableId", tableId)
                    .uniqueResult();
              },
              "read commits",
              true,
              Optional.of(Connection.TRANSACTION_REPEATABLE_READ));

      // Must see version 2 — not a stale snapshot showing only version 1
      assertThat(maxVersion).isEqualTo(2L);
    }

    private DeltaCommitDAO buildCommitDAO(UUID tableId, long version) {
      return DeltaCommitDAO.builder()
          .tableId(tableId)
          .commitVersion(version)
          .commitFilename(version + ".json")
          .commitFilesize(100)
          .commitFileModificationTimestamp(new Date())
          .commitTimestamp(new Date())
          .isBackfilledLatestCommit(false)
          .build();
    }
  }
}
