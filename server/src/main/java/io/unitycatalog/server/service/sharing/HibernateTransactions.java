package io.unitycatalog.server.service.sharing;

import io.opensharing.Transactions;
import jakarta.persistence.EntityManager;
import java.util.function.Function;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

/**
 * Runs OpenSharing's store work in Hibernate sessions of Unity Catalog's session factory, the way
 * UC's own repositories do. Not built on {@code TransactionManager.executeWithTransaction}: that
 * rethrows anything other than a UC {@code BaseException} as an internal error, which would turn
 * OpenSharing's not-found, already-exists and permission errors into 500s.
 *
 * <p>Work started while the calling thread is already in one of these transactions joins it, so a
 * caller can run several OpenSharing calls and its own steps as one unit: all of it commits, or
 * none of it does.
 */
public final class HibernateTransactions implements Transactions {

  private final SessionFactory sessionFactory;
  private final ThreadLocal<Session> current = new ThreadLocal<>();

  public HibernateTransactions(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public <T> T inTransaction(boolean readOnly, Function<EntityManager, T> work) {
    Session joined = current.get();
    if (joined != null) {
      // The outer transaction decides: a failure here propagates and rolls all of it back.
      return work.apply(joined);
    }
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(readOnly);
      Transaction tx = session.beginTransaction();
      current.set(session);
      try {
        T result = work.apply(session);
        tx.commit();
        return result;
      } catch (RuntimeException e) {
        if (tx.isActive()) {
          tx.rollback();
        }
        throw e;
      } finally {
        current.remove();
      }
    }
  }
}
