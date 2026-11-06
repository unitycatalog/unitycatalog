package io.unitycatalog.server.sharing;

import io.opensharing.Transactions;
import jakarta.persistence.EntityManager;
import java.util.function.Function;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

/**
 * Runs OpenSharing's store work in Hibernate sessions, the way UC's own repositories do. Not built
 * on {@code TransactionManager.executeWithTransaction}: that rethrows anything other than a UC
 * {@code BaseException} as an internal error, which would turn OpenSharing's not-found,
 * already-exists and permission errors into 500s.
 */
final class HibernateTransactions implements Transactions {

  private final SessionFactory sessionFactory;

  HibernateTransactions(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public <T> T inTransaction(boolean readOnly, Function<EntityManager, T> work) {
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(readOnly);
      Transaction tx = session.beginTransaction();
      try {
        T result = work.apply(session);
        tx.commit();
        return result;
      } catch (RuntimeException e) {
        if (tx.isActive()) {
          tx.rollback();
        }
        throw e;
      }
    }
  }
}
