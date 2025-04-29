package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
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
    try (Session session = sessionFactory.openSession()) {
      if (readOnly) {
        session.setDefaultReadOnly(true);
      }
      Transaction tx = session.beginTransaction();
      try {
        R result = operation.execute(session);
        tx.commit();
        return result;
      } catch (Exception e) {
        tx.rollback();
        LOGGER.error(errorMessage, e);
        if (e instanceof BaseException) {
          throw (BaseException) e;
        }
        throw new BaseException(ErrorCode.INTERNAL, errorMessage + ": " + e.getMessage());
      }
    }
  }
}
