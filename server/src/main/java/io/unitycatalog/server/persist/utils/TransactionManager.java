package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.sql.Connection;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.LockAcquisitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for managing database transactions. */
public class TransactionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionManager.class);

  /** Maximum number of times a transaction is tried (including the first attempt). */
  private static final int MAX_TRANSACTION_TRY_COUNT = 3;

  /**
   * Functional interface for database operations that return a result.
   *
   * @param <R> the result type
   */
  @FunctionalInterface
  public interface DatabaseOperation<R> {
    R execute(Session session) throws Exception;
  }

  /**
   * Executes a database operation with transaction management.
   *
   * <p><b>Read transactions</b> ({@code readOnly=true}) run at {@code REPEATABLE_READ} isolation.
   * This ensures a stable snapshot for the entire transaction so that multiple reads within the
   * same transaction see consistent data. Without it, a {@code READ_COMMITTED} database (such as
   * H2's default) refreshes its snapshot on every statement, which can cause a single logical
   * operation that issues multiple queries (e.g. listing commits while new commits are being
   * posted) to observe version numbers that appear to go backwards or skip, breaking monotonicity
   * assumptions.
   *
   * <p><b>Write transactions</b> ({@code readOnly=false}) run at {@code SERIALIZABLE} isolation. On
   * production databases that implement Serializable Snapshot Isolation (SSI), such as PostgreSQL,
   * this detects dangerous read-write anti-dependency cycles between concurrent transactions and
   * aborts one of them, preventing phantom writes. The aborted transaction is retried by this
   * method so that the business logic re-runs with a fresh snapshot and produces the correct
   * application-level error (e.g. {@code ALREADY_EXISTS} or {@code NOT_FOUND}) rather than a raw
   * database error. Unique constraints on the entity tables serve as an additional safety net that
   * catches concurrent duplicate inserts even when SSI is unavailable (e.g. H2).
   *
   * <p>On a serialization conflict ({@link LockAcquisitionException}) or unique-constraint
   * violation ({@link ConstraintViolationException}) the transaction is tried up to {@link
   * #MAX_TRANSACTION_TRY_COUNT} times.
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
    int isolationLevel =
        readOnly ? Connection.TRANSACTION_REPEATABLE_READ : Connection.TRANSACTION_SERIALIZABLE;
    for (int attempt = 0; attempt < MAX_TRANSACTION_TRY_COUNT; attempt++) {
      try (Session session = sessionFactory.openSession()) {
        if (readOnly) {
          session.setDefaultReadOnly(true);
        }

        // Save and set custom isolation level
        final int[] originalIsolation = new int[1];
        session.doWork(
            connection -> {
              originalIsolation[0] = connection.getTransactionIsolation();
              connection.setTransactionIsolation(isolationLevel);
            });

        Transaction tx = session.beginTransaction();
        try {
          R result = operation.execute(session);
          tx.commit();
          return result;
        } catch (LockAcquisitionException | ConstraintViolationException e) {
          tx.rollback();
          if (attempt < MAX_TRANSACTION_TRY_COUNT - 1) {
            LOGGER.debug(
                "Serialization conflict on attempt {}/{}, retrying: {}",
                attempt + 1,
                MAX_TRANSACTION_TRY_COUNT,
                errorMessage);
          } else {
            LOGGER.warn(
                "Transaction aborted after {} attempt(s) due to serialization conflict: {}",
                MAX_TRANSACTION_TRY_COUNT,
                errorMessage);
          }
        } catch (Exception e) {
          tx.rollback();
          LOGGER.debug(errorMessage, e);
          if (e instanceof BaseException) {
            throw (BaseException) e;
          }
          throw new BaseException(ErrorCode.INTERNAL, errorMessage, e);
        } finally {
          // Restore original isolation level to avoid polluting connection pool
          session.doWork(
              connection -> {
                connection.setTransactionIsolation(originalIsolation[0]);
              });
        }
      }
    }
    throw new BaseException(
        ErrorCode.INTERNAL, errorMessage + ": serialization conflict, max retries exceeded");
  }
}
