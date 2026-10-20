package io.unitycatalog.server.persist.utils;

import static io.unitycatalog.server.persist.utils.TransactionManagerTest.MockTransactionTest.OperationError.CLIENT_ERROR;
import static io.unitycatalog.server.persist.utils.TransactionManagerTest.MockTransactionTest.OperationError.RUNTIME;
import static io.unitycatalog.server.persist.utils.TransactionManagerTest.MockTransactionTest.OperationError.SIGNAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.TransactionRollbackException;
import io.unitycatalog.server.persist.dao.DeltaCommitDAO;
import io.unitycatalog.server.utils.ServerProperties;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.exception.GenericJDBCException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.INTERNAL)
          .hasMessageContaining(errorMessage)
          .hasMessageContaining(testException.getMessage())
          .hasCause(testException);

      verify(transaction).rollback();
      verify(transaction, never()).commit();
    }

    @Test
    public void testTransactionRollbackExceptionIsRethrownAsIsNotWrapped() {
      // A TransactionRollbackException signals rollback, not an error, so it is rethrown by
      // identity for the caller to catch, not wrapped into a BaseException/INTERNAL.
      SignalException signal = new SignalException();

      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory,
                      session -> {
                        throw signal;
                      },
                      "Error executing operation",
                      false))
          .isSameAs(signal);

      verify(transaction).rollback();
      verify(transaction, never()).commit();
    }

    /** A replay signal, the control-flow exception that callers treat as success. */
    static class SignalException extends TransactionRollbackException {}

    /** What the operation fails with. */
    enum OperationError {
      RUNTIME,
      CLIENT_ERROR,
      SIGNAL
    }

    /**
     * One row per combination of failures after the operation fails. Columns: the operation's
     * error, whether the rollback fails, whether the isolation restore fails; then what is thrown
     * ({@code original}, {@code internal} wrapping it, or {@code rollback failed} wrapping a
     * signal), the failures suppressed on the thrown error, and those suppressed on the original.
     */
    static Stream<Arguments> failureLevels() {
      return Stream.of(
          Arguments.of(RUNTIME, false, false, "internal", List.of(), List.of()),
          Arguments.of(RUNTIME, true, false, "internal", List.of(), List.of("rollback")),
          Arguments.of(RUNTIME, false, true, "internal", List.of("restore"), List.of()),
          Arguments.of(RUNTIME, true, true, "internal", List.of("restore"), List.of("rollback")),
          Arguments.of(CLIENT_ERROR, false, false, "original", List.of(), List.of()),
          Arguments.of(
              CLIENT_ERROR, true, false, "original", List.of("rollback"), List.of("rollback")),
          Arguments.of(
              CLIENT_ERROR, false, true, "original", List.of("restore"), List.of("restore")),
          Arguments.of(
              CLIENT_ERROR,
              true,
              true,
              "original",
              List.of("rollback", "restore"),
              List.of("rollback", "restore")),
          Arguments.of(SIGNAL, false, false, "original", List.of(), List.of()),
          // The caller takes the signal as success, so a failed restore after it goes unseen.
          Arguments.of(SIGNAL, false, true, "original", List.of("restore"), List.of("restore")),
          Arguments.of(SIGNAL, true, false, "rollback failed", List.of(), List.of("rollback")),
          Arguments.of(
              SIGNAL, true, true, "rollback failed", List.of("restore"), List.of("rollback")));
    }

    @ParameterizedTest
    @MethodSource("failureLevels")
    public void testFailuresAfterTheOperationAreKeptOnItsError(
        OperationError operationError,
        boolean rollbackFails,
        boolean restoreFails,
        String thrown,
        List<String> suppressedOnThrown,
        List<String> suppressedOnOriginal) {
      Exception original =
          switch (operationError) {
            case RUNTIME -> new RuntimeException("connection reset");
            case CLIENT_ERROR -> new BaseException(ErrorCode.INVALID_ARGUMENT, "bad input");
            case SIGNAL -> new SignalException();
          };
      Map<String, RuntimeException> failures =
          Map.of(
              "rollback", new RuntimeException("rollback failed"),
              "restore", new RuntimeException("restore failed"));
      if (rollbackFails) {
        doThrow(failures.get("rollback")).when(transaction).rollback();
      }
      if (restoreFails) {
        // The first doWork sets the isolation level; the second restores it.
        doNothing().doThrow(failures.get("restore")).when(session).doWork(any());
      }

      Throwable error =
          catchThrowable(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory,
                      session -> {
                        throw original;
                      },
                      "Error executing operation",
                      false,
                      Optional.of(Connection.TRANSACTION_REPEATABLE_READ)));

      switch (thrown) {
        case "original" -> assertThat(error).isSameAs(original);
        case "internal" -> assertInternal(error, "Error executing operation: connection reset");
        case "rollback failed" ->
            assertInternal(error, "Error executing operation: transaction rollback failed");
        default -> throw new IllegalArgumentException(thrown);
      }
      if (error != original) {
        assertThat(error).hasCause(original);
      }
      assertThat(error.getSuppressed())
          .containsExactly(
              suppressedOnThrown.stream().map(failures::get).toArray(Throwable[]::new));
      assertThat(original.getSuppressed())
          .containsExactly(
              suppressedOnOriginal.stream().map(failures::get).toArray(Throwable[]::new));
      verify(transaction).rollback();
      verify(transaction, never()).commit();
    }

    private void assertInternal(Throwable error, String message) {
      assertThat(error)
          .isInstanceOfSatisfying(
              BaseException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INTERNAL))
          .hasMessage(message);
    }

    @Test
    public void testEveryFailureAfterTheOperationIsKeptInOrder() {
      RuntimeException error = new RuntimeException("connection reset");
      RuntimeException rollbackError = new RuntimeException("rollback failed");
      RuntimeException restoreError = new RuntimeException("restore failed");
      RuntimeException closeError = new RuntimeException("close failed");
      doThrow(rollbackError).when(transaction).rollback();
      doNothing().doThrow(restoreError).when(session).doWork(any());
      doThrow(closeError).when(session).close();

      Throwable thrown =
          catchThrowable(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory,
                      session -> {
                        throw error;
                      },
                      "Error executing operation",
                      false,
                      Optional.of(Connection.TRANSACTION_REPEATABLE_READ)));

      // The restore runs before the session closes, so its failure is attached first.
      assertInternal(thrown, "Error executing operation: connection reset");
      assertThat(thrown).hasCause(error);
      assertThat(thrown.getSuppressed()).containsExactly(restoreError, closeError);
      assertThat(error.getSuppressed()).containsExactly(rollbackError);
    }

    @Test
    public void testFailedCommitIsRolledBackAndKeptAsTheCause() {
      RuntimeException commitError = new RuntimeException("commit failed");
      doThrow(commitError).when(transaction).commit();

      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory, session -> "ok", "Error executing operation", false))
          .isInstanceOfSatisfying(
              BaseException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INTERNAL))
          .hasMessage("Error executing operation: commit failed")
          .hasCause(commitError);
      verify(transaction).rollback();
    }

    @Test
    public void testFailedIsolationRestoreAfterACommitIsThrown() {
      RuntimeException restoreError = new RuntimeException("restore failed");
      doNothing().doThrow(restoreError).when(session).doWork(any());

      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory,
                      session -> "ok",
                      "Error executing operation",
                      false,
                      Optional.of(Connection.TRANSACTION_REPEATABLE_READ)))
          .isSameAs(restoreError);
      verify(transaction).commit();
      verify(transaction, never()).rollback();
    }

    @Test
    public void testFailedSessionCloseAfterACommitIsThrown() {
      RuntimeException closeError = new RuntimeException("close failed");
      doThrow(closeError).when(session).close();

      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory, session -> "ok", "Error executing operation", false))
          .isSameAs(closeError);
      assertThat(closeError.getSuppressed()).isEmpty();
      verify(transaction).commit();
    }

    @Test
    public void testFailedRestoreAndCloseAfterACommitKeepTheRestoreError() {
      RuntimeException restoreError = new RuntimeException("restore failed");
      RuntimeException closeError = new RuntimeException("close failed");
      doNothing().doThrow(restoreError).when(session).doWork(any());
      doThrow(closeError).when(session).close();

      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory,
                      session -> "ok",
                      "Error executing operation",
                      false,
                      Optional.of(Connection.TRANSACTION_REPEATABLE_READ)))
          .isSameAs(restoreError);
      assertThat(restoreError.getSuppressed()).containsExactly(closeError);
      verify(transaction).commit();
    }

    @Test
    public void testFailedIsolationSetFailsBeforeTheTransaction() {
      RuntimeException setError = new RuntimeException("set failed");
      reset(session);
      doThrow(setError).when(session).doWork(any());

      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory,
                      session -> "ok",
                      "Error executing operation",
                      false,
                      Optional.of(Connection.TRANSACTION_REPEATABLE_READ)))
          .isSameAs(setError);
      // Only the set ran: no restore after a failed set.
      verify(session, times(1)).doWork(any());
      verify(session, never()).beginTransaction();
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
                      /* readOnly= */ true))
          .isInstanceOf(BaseException.class)
          .hasMessageContaining(errorMessage)
          .hasMessageContaining(hibernateException.getMessage());

      // Verify the transaction was rolled back
      verify(transaction).rollback();
      verify(transaction, never()).commit();

      // Verify session was set to read-only
      verify(session).setDefaultReadOnly(true);
    }

    @Test
    public void testDatabaseErrorWithoutSqlStateIsInternal() {
      GenericJDBCException databaseError =
          new GenericJDBCException(
              "could not execute statement", new SQLException("database error"));

      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory,
                      session -> {
                        throw databaseError;
                      },
                      "Error executing operation",
                      false))
          .isInstanceOf(BaseException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.INTERNAL)
          .hasMessage("Error executing operation: " + databaseError.getMessage());
      // Without a SQLSTATE there is nothing to look up, so the dialect is not read.
      verify(sessionFactory, never()).unwrap(any());
    }

    @ParameterizedTest
    @MethodSource("io.unitycatalog.server.persist.utils.TransactionManagerTest#databaseErrors")
    public void testDatabaseErrorCode(
        Class<? extends Dialect> dialect, String sqlState, ErrorCode expectedErrorCode) {
      SessionFactoryImplementor sessionFactoryImplementor = mock(SessionFactoryImplementor.class);
      JdbcServices jdbcServices = mock(JdbcServices.class);
      when(sessionFactory.unwrap(SessionFactoryImplementor.class))
          .thenReturn(sessionFactoryImplementor);
      when(sessionFactoryImplementor.getJdbcServices()).thenReturn(jdbcServices);
      when(jdbcServices.getDialect()).thenReturn(mock(dialect));
      GenericJDBCException databaseError =
          new GenericJDBCException(
              "could not execute statement", new SQLException("database error", sqlState));

      assertThatThrownBy(
              () ->
                  TransactionManager.executeWithTransaction(
                      sessionFactory,
                      session -> {
                        throw databaseError;
                      },
                      "Error executing operation",
                      false))
          .isInstanceOf(BaseException.class)
          .hasFieldOrPropertyWithValue("errorCode", expectedErrorCode)
          .hasMessage("Error executing operation: " + databaseError.getMessage());
      verify(transaction).rollback();
    }
  }

  /**
   * A listed (database, SQLSTATE) pair gets its error code; every other database error is INTERNAL.
   */
  static Stream<Arguments> databaseErrors() {
    return Stream.of(
        Arguments.of(PostgreSQLDialect.class, "54000", ErrorCode.INVALID_ARGUMENT),
        Arguments.of(H2Dialect.class, "54000", ErrorCode.INTERNAL),
        Arguments.of(PostgreSQLDialect.class, "23505", ErrorCode.INTERNAL));
  }

  /**
   * Tests for the isolation level code path using a real H2 database. These verify that the
   * rollback() call before setTransactionIsolation() correctly flushes stale implicit transactions
   * from pooled connections, ensuring REPEATABLE_READ gets a fresh MVCC snapshot.
   */
  @Nested
  public class IsolationLevelWithRealDatabaseTest {

    private static SessionFactory realSessionFactory;
    private static HibernateConfigurator hibernateConfigurator;

    @BeforeAll
    public static void setUp() {
      ServerProperties serverProperties = new ServerProperties(new Properties());
      hibernateConfigurator = new HibernateConfigurator(serverProperties);
      realSessionFactory = hibernateConfigurator.getSessionFactory();
    }

    @AfterAll
    public static void tearDown() {
      hibernateConfigurator.close();
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
