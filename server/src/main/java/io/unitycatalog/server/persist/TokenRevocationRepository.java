package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.dao.TokenRevocationDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * <p>Rows are removed once their expiry is more than {@link #RETENTION_GRACE} in the past. That
 * cleanup runs in its own transaction after the revocation commits (see {@link #revoke}), so it
 * never rolls back or delays the single-row revocation write.
 *
 * <p>Database failures are thrown without the database's message, which the error handler logs with
 * the cause, since these methods back logout and token exchange.
 */
public class TokenRevocationRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(TokenRevocationRepository.class);

  /**
   * How long a row is kept past its token's expiry. {@code expires_at} has no time zone, so a
   * server in another time zone, or a daylight saving change, can shift the stored value by up to a
   * day; the grace keeps such a row until its token has surely expired.
   */
  static final Duration RETENTION_GRACE = Duration.ofDays(2);

  private final SessionFactory sessionFactory;
  // Set while a cleanup sweep is running, so a burst of logouts starts one sweep, not one each.
  private final AtomicBoolean isCleanupRunning = new AtomicBoolean();

  public TokenRevocationRepository(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  /**
   * Records a token's jti as revoked. This touches only the token's own row, so concurrent
   * revocations of different sessions do not contend.
   *
   * <p>Idempotent under concurrency: if the same token is revoked twice at once, one insert wins
   * and the other fails on the jti primary key; that is treated as success, since the token is
   * revoked either way. The first stored expiry is kept.
   *
   * <p>After the revocation commits, an asynchronous, best-effort cleanup of expired rows is
   * started unless one is already running. A failed sweep is harmless, since an expired row can
   * never match a live token and the next revocation will retry the cleanup.
   *
   * @param jti the revoked token's JWT ID
   * @param expiresAt the token's expiry, after which the row may be removed; {@code null} means the
   *     token never expires, so the revocation is permanent
   * @throws BaseException if the database fails
   */
  public void revoke(UUID jti, Date expiresAt) {
    try {
      TransactionManager.executeWithTransaction(
          sessionFactory,
          session -> {
            session.persist(TokenRevocationDAO.builder().jti(jti).expiresAt(expiresAt).build());
            return null;
          },
          "Failed to revoke token",
          /* readOnly= */ false);
      LOGGER.debug("Revoked token jti={} until {}", jti, expiresAt);
    } catch (BaseException e) {
      // A concurrent logout of the same token can insert the row first, failing this transaction on
      // the jti primary key. The token is revoked either way, so ignore the failure once the row is
      // present. The cleanup may also have purged that row by now, but only once the token has
      // expired, and an expired token is rejected anyway. Rethrow anything else.
      if (!isRevoked(jti) && !isExpired(expiresAt)) {
        throw withoutDatabaseMessage(e, "Failed to revoke token");
      }
      LOGGER.debug("Token jti={} was already revoked; treating this revoke as a no-op", jti);
    }

    if (isCleanupRunning.compareAndSet(false, true)) {
      CompletableFuture.runAsync(this::deleteExpiredQuietly);
    }
  }

  /**
   * Removes revocation rows whose expiry is more than {@link #RETENTION_GRACE} in the past, in its
   * own transaction. An expired token is already rejected by expiration checking, so its denylist
   * row is only taking up space by the time this removes it. Rows with a {@code null} expiry are
   * permanent and never removed.
   *
   * <p>Package-private for testing; callers rely on the cleanup triggered by {@link #revoke}.
   *
   * @return the number of rows deleted
   */
  int deleteExpired() {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          MutationQuery purge =
              session.createMutationQuery(
                  "DELETE FROM TokenRevocationDAO WHERE expiresAt < :cutoff");
          purge.setParameter("cutoff", Date.from(Instant.now().minus(RETENTION_GRACE)));
          return purge.executeUpdate();
        },
        "Failed to delete expired token revocations",
        /* readOnly= */ false);
  }

  /** Whether a token with this expiry has expired, the only case where its row may be purged. */
  private static boolean isExpired(Date expiresAt) {
    return expiresAt != null && expiresAt.before(new Date());
  }

  private void deleteExpiredQuietly() {
    try {
      deleteExpired();
    } catch (Exception e) {
      LOGGER.debug("Best-effort cleanup of expired token revocations failed", e);
    } finally {
      isCleanupRunning.set(false);
    }
  }

  /**
   * Returns whether the given jti has been revoked. Session-scoped so it can share the caller's
   * transaction (the authenticated request path checks this alongside the user lookup in one
   * transaction). It is a single primary-key lookup, as it runs on every request.
   *
   * @param session the caller's open session
   * @param jti the token's JWT ID
   * @return {@code true} if the token has been revoked
   */
  public boolean isRevoked(Session session, UUID jti) {
    return session.get(TokenRevocationDAO.class, jti) != null;
  }

  /**
   * Returns whether the given jti has been revoked, in its own read-only transaction. For callers
   * that have no session of their own, such as token exchange.
   *
   * @param jti the token's JWT ID
   * @return {@code true} if the token has been revoked
   * @throws BaseException if the database fails
   */
  public boolean isRevoked(UUID jti) {
    String errorMessage = "Failed to check token revocation";
    try {
      return TransactionManager.executeWithTransaction(
          sessionFactory, session -> isRevoked(session, jti), errorMessage, /* readOnly= */ true);
    } catch (BaseException e) {
      throw withoutDatabaseMessage(e, errorMessage);
    }
  }

  /**
   * Returns {@code e} with only {@code message}: {@link TransactionManager} appends the database's
   * message, which stays on the cause for the server log.
   */
  public static BaseException withoutDatabaseMessage(BaseException e, String message) {
    return new BaseException(e.getErrorCode(), message, e);
  }
}
