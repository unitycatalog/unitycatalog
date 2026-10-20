package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.TransactionRollbackException;
import java.util.Optional;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for managing database transactions. */
public class TransactionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionManager.class);

  /**
   * Functional interface for database operations that return a result.
   *
   * @param <R> the result type
   */
  @FunctionalInterface
  public interface DatabaseOperation<R> {
    R execute(Session session) throws Exception;
  }

  /** Restores the connection's isolation level when the transaction ends. */
  private interface IsolationRestore extends AutoCloseable {
    @Override
    void close();
  }

  /**
   * Executes a database operation with transaction management.
   *
   * @param sessionFactory the Hibernate session factory
   * @param operation the database operation to execute
   * @param errorMessage the error message to use if the operation fails
   * @param readOnly whether the session should be read-only
   * @param <R> the result type
   * @return the result of the operation
   */
  public static <R> R executeWithTransaction(
      SessionFactory sessionFactory,
      DatabaseOperation<R> operation,
      String errorMessage,
      boolean readOnly) {
    return executeWithTransaction(
        sessionFactory, operation, errorMessage, readOnly, Optional.empty());
  }

  /**
   * Executes a database operation with transaction management and optional custom isolation level.
   *
   * <p>A failure to set the isolation level or begin the transaction is thrown as-is, as is a
   * failure to restore the isolation level or close the session after a commit. A failed rollback
   * is added as suppressed to the operation's error, and a failed isolation restore or session
   * close to the exception thrown. When that exception is a 4xx or a {@link
   * TransactionRollbackException}, which the error handler does not log, it is logged as a warning
   * with these failures.
   *
   * @param sessionFactory the Hibernate session factory
   * @param operation the database operation to execute
   * @param errorMessage the error message to use if the operation fails
   * @param readOnly whether the session should be read-only
   * @param isolationLevel optional transaction isolation level (e.g.,
   *     Connection.TRANSACTION_REPEATABLE_READ). If present, the original isolation level will be
   *     saved and restored after the transaction to avoid affecting pooled connections.
   * @param <R> the result type
   * @return the result of the operation
   * @throws BaseException if the operation or the commit fails: a {@link BaseException} is rethrown
   *     as-is, and any other exception is wrapped with the code from {@link DatabaseErrors}
   *     ({@code INTERNAL} by default) with it as the cause.
   * @throws TransactionRollbackException rethrown as-is once its rollback succeeds; if the rollback
   *     fails, it is wrapped as {@code INTERNAL} instead.
   */
  public static <R> R executeWithTransaction(
      SessionFactory sessionFactory,
      DatabaseOperation<R> operation,
      String errorMessage,
      boolean readOnly,
      Optional<Integer> isolationLevel) {
    try (Session session = sessionFactory.openSession()) {
      if (readOnly) {
        session.setDefaultReadOnly(true);
      }

      // Save and set custom isolation level if specified.
      // IMPORTANT: without autocommit, a pooled connection may carry an open implicit transaction
      // from its prior use. H2's setTransactionIsolation() takes effect for the NEXT transaction,
      // not the current implicit one, so that transaction has to end first or REPEATABLE_READ
      // reads can see stale data from before the connection was returned to the pool. MySQL and
      // PostgreSQL reject rollback() on an autocommit connection, so only roll back when there is
      // something to end.
      final int[] originalIsolation = new int[1];
      if (isolationLevel.isPresent()) {
        session.doWork(
            connection -> {
              if (!connection.getAutoCommit()) {
                connection.rollback();
              }
              originalIsolation[0] = connection.getTransactionIsolation();
              connection.setTransactionIsolation(isolationLevel.get());
            });
      }

      // Restore original isolation level to avoid polluting connection pool. As a resource, a
      // failed restore is added to the error in flight as suppressed instead of replacing it.
      IsolationRestore restore =
          () -> {
            if (isolationLevel.isPresent()) {
              session.doWork(
                  connection -> connection.setTransactionIsolation(originalIsolation[0]));
            }
          };
      try (restore) {
        Transaction tx = session.beginTransaction();
        try {
          R result = operation.execute(session);
          tx.commit();
          return result;
        } catch (Exception e) {
          throw rollBack(sessionFactory, tx, e, errorMessage);
        }
      }
    } catch (RuntimeException e) {
      warnIfCleanupFailureIsUnlogged(e, errorMessage);
      throw e;
    }
  }

  /**
   * Logs the rollback, restore or close failures suppressed on {@code e} when nothing else logs
   * them: the error handler logs only a 5xx, and the caller takes a signal as a normal outcome.
   */
  private static void warnIfCleanupFailureIsUnlogged(RuntimeException e, String errorMessage) {
    boolean unlogged =
        e instanceof TransactionRollbackException
            || (e instanceof BaseException baseException
                && !baseException.getErrorCode().getHttpStatus().isServerError());
    if (unlogged && e.getSuppressed().length > 0) {
      LOGGER.warn("{}: cleanup after the transaction failed", errorMessage, e);
    }
  }

  /**
   * Rolls back {@code tx} after {@code e} and returns the exception to throw: {@code e} itself if
   * it is a {@link BaseException} or a {@link TransactionRollbackException}, otherwise an error
   * with the code from {@link DatabaseErrors} ({@code INTERNAL} by default) and {@code e} as the
   * cause. A failed rollback is added to {@code e} as
   * suppressed, and turns a {@link TransactionRollbackException} into an {@code INTERNAL} error
   * with {@code e} as the cause.
   */
  private static RuntimeException rollBack(
      SessionFactory sessionFactory, Transaction tx, Exception e, String errorMessage) {
    try {
      tx.rollback();
    } catch (RuntimeException rollbackError) {
      e.addSuppressed(rollbackError);
      // Callers act on a signal as if its work was undone. The rollback failed, so fail as
      // INTERNAL instead.
      if (e instanceof TransactionRollbackException) {
        return new BaseException(
            ErrorCode.INTERNAL, errorMessage + ": transaction rollback failed", e);
      }
    }
    // A control-flow signal (e.g. an idempotent-replay short-circuit) is not an error: rethrow it
    // as-is, without error logging, so the caller can catch its specific type. The rollback above
    // has already undone its work.
    if (e instanceof TransactionRollbackException signal) {
      return signal;
    }
    LOGGER.debug(errorMessage, e);
    if (e instanceof BaseException baseException) {
      return baseException;
    }
    ErrorCode errorCode = DatabaseErrors.errorCode(sessionFactory, e).orElse(ErrorCode.INTERNAL);
    return new BaseException(errorCode, errorMessage + ": " + e.getMessage(), e);
  }
}
