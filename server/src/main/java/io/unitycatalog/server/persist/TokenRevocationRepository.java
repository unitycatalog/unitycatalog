package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.dao.TokenRevocationDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.MutationQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the JWT IDs (jti) of access tokens that have been revoked by logout.
 *
 * <p>The denylist is stored in the database and {@link #isRevoked} is consulted directly on the
 * authenticated request path, so a revoked token is rejected on its next request and there is no
 * in-memory cache to keep in sync. The per-request cost is a single primary-key lookup on a small
 * table, on the same connection the request already uses for its user lookup.
 *
 * <p>The table is kept bounded by removing rows once their retained expiry has passed. That cleanup
 * runs off the revocation path (see {@link #revoke}) so a table-wide delete never contends with, or
 * delays, the single-row revocation write.
 */
public class TokenRevocationRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(TokenRevocationRepository.class);
  private final SessionFactory sessionFactory;

  public TokenRevocationRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  /**
   * Records a token's jti as revoked. A {@code null} {@code expiresAt} means the token has no
   * expiry, so its revocation is permanent; otherwise the row is kept until {@code expiresAt}. This
   * touches only the token's own row, so concurrent revocations of different sessions do not
   * contend.
   *
   * <p>Idempotent under concurrency: if the same token is revoked twice at once, one insert wins
   * and the other fails on the jti primary key; that is treated as success, since the token is
   * revoked either way. The expiry is derived from the token, so both inserts carry the same value
   * and there is nothing to reconcile.
   *
   * <p>After the revocation commits, an asynchronous, best-effort cleanup of expired rows is
   * triggered in a separate transaction. It is deliberately fire-and-forget: a failed sweep (e.g.
   * lock contention with another sweep) is harmless, since an expired row can never match a live
   * token and the next revocation will retry the cleanup.
   */
  public void revoke(UUID jti, Date expiresAt) {
    try {
      TransactionManager.executeWithTransaction(
          sessionFactory,
          session -> {
            session.persist(TokenRevocationDAO.builder().jti(jti).expiresAt(expiresAt).build());
            LOGGER.debug("Revoked token jti={} until {}", jti, expiresAt);
            return null;
          },
          "Failed to revoke token",
          /* readOnly= */ false);
    } catch (BaseException e) {
      // A concurrent logout of the same token can insert the row first, failing this transaction on
      // the jti primary key. The token is revoked either way, so ignore the failure once the row is
      // present, and rethrow anything else.
      if (!isRevoked(jti)) {
        throw e;
      }
    }

    CompletableFuture.runAsync(this::deleteExpiredQuietly);
  }

  /**
   * Removes revocation rows whose retained expiry has passed, in its own transaction. Returns the
   * number of rows deleted. An expired token is already rejected by expiration checking, so its
   * denylist row is only taking up space by the time this removes it.
   *
   * <p>Public for testing; callers should rely on the automatic cleanup triggered by {@link
   * #revoke}.
   */
  public int deleteExpired() {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          MutationQuery purge =
              session.createMutationQuery("DELETE FROM TokenRevocationDAO WHERE expiresAt < :now");
          purge.setParameter("now", new Date());
          return purge.executeUpdate();
        },
        "Failed to delete expired token revocations",
        /* readOnly= */ false);
  }

  private void deleteExpiredQuietly() {
    try {
      deleteExpired();
    } catch (Exception e) {
      LOGGER.debug("Best-effort cleanup of expired token revocations failed", e);
    }
  }

  /**
   * Returns whether the given jti has been revoked. Session-scoped so it can share the caller's
   * transaction (the authenticated request path checks this alongside the user lookup in one
   * transaction). It is a single primary-key lookup, as it runs on every request.
   */
  public boolean isRevoked(Session session, UUID jti) {
    return session.get(TokenRevocationDAO.class, jti) != null;
  }

  private boolean isRevoked(UUID jti) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> isRevoked(session, jti),
        "Failed to check token revocation",
        /* readOnly= */ true);
  }
}
