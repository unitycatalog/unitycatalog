package io.unitycatalog.server.auth;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.hibernate.SessionFactory;
import org.hibernate.query.NativeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reloads the in-memory Casbin policy when {@code casbin_rule} changes in the shared database.
 *
 * <p>{@code casbin_rule} is the JDBC adapter table that persists Casbin policy and grouping rows
 * (grants, revokes, and hierarchy). {@link JCasbinAuthorizer} writes through the adapter with
 * auto-save enabled, but each server process only loads that table into its enforcer at startup
 * unless this refresher reloads it.
 *
 * <p>Change detection uses {@code select count(*), coalesce(max(id), 0) from casbin_rule}. The JDBC
 * adapter only inserts and deletes rows with auto-incrementing ids, so any write changes at least
 * one of the two values. Reload is delegated to {@link JCasbinAuthorizer} so a fresh enforcer can
 * be built and swapped without blocking {@code enforce()} on the live instance.
 */
public class CasbinPolicyRefresher implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CasbinPolicyRefresher.class);

  private static final String VERSION_QUERY =
      "select count(*), coalesce(max(id), 0) from casbin_rule";

  private final Runnable reloader;
  private final SessionFactory sessionFactory;

  // Initial -1 forces a reload on first check (see recordVersion).
  private long lastCount = -1;
  private long lastMaxId = -1;

  /**
   * Advanced only after a consistent check (no-op or successful reload). Callers that wait on the
   * monitor while another thread finishes can skip by comparing against a pre-lock snapshot.
   */
  private final AtomicLong checkSeq = new AtomicLong();

  private ScheduledExecutorService executor;

  public CasbinPolicyRefresher(Runnable reloader, SessionFactory sessionFactory) {
    this.reloader = reloader;
    this.sessionFactory = sessionFactory;
  }

  public synchronized void start(Duration interval) {
    if (executor != null) {
      return;
    }
    executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "casbin-policy-refresher");
              thread.setDaemon(true);
              return thread;
            });
    long millis = Math.max(1, interval.toMillis());
    executor.scheduleWithFixedDelay(this::pollQuietly, millis, millis, TimeUnit.MILLISECONDS);
    LOGGER.info("Casbin policy refresh enabled, checking every {}ms", millis);
  }

  private void pollQuietly() {
    try {
      checkAndReload();
    } catch (Throwable t) {
      LOGGER.warn("Casbin policy refresh check failed; will retry on the next interval", t);
    }
  }

  /**
   * Probes {@code casbin_rule} and reloads when the version changed.
   *
   * <p>Probe, reload, and version stamp run under one lock so concurrent callers coalesce: a waiter
   * that blocked while another thread completed a consistent check skips. The stamped version is
   * the one probed before reload, so the cache matches what was loaded.
   *
   * @return true if a reload happened
   */
  public boolean checkAndReload() {
    long seenCheck = checkSeq.get();
    synchronized (this) {
      if (checkSeq.get() != seenCheck) {
        return false;
      }

      long[] db = readVersion().orElse(null);
      if (db == null) {
        // Probe failed: do not advance checkSeq so waiters retry.
        return false;
      }
      if (db[0] == lastCount && db[1] == lastMaxId) {
        checkSeq.incrementAndGet();
        return false;
      }

      LOGGER.debug(
          "Casbin policy changed (count {} -> {}, maxId {} -> {}); reloading",
          lastCount,
          db[0],
          lastMaxId,
          db[1]);

      // May throw: leave checkSeq alone so waiters retry.
      reloader.run();
      // Stamp the probed version, not a post-reload re-read: a fresher DB stamp would claim we
      // loaded state the new enforcer may not contain.
      recordVersion(db[0], db[1]);
      checkSeq.incrementAndGet();
      return true;
    }
  }

  private void recordVersion(long count, long maxId) {
    lastCount = count;
    lastMaxId = maxId;
  }

  private Optional<long[]> readVersion() {
    try {
      return sessionFactory.fromSession(
          session -> {
            NativeQuery<?> query = session.createNativeQuery(VERSION_QUERY);
            Object[] row = (Object[]) query.getSingleResult();
            return Optional.of(
                new long[] {((Number) row[0]).longValue(), ((Number) row[1]).longValue()});
          });
    } catch (Exception e) {
      LOGGER.debug("Could not read the Casbin policy version", e);
      return Optional.empty();
    }
  }

  @Override
  public void close() {
    ScheduledExecutorService toShutDown;
    synchronized (this) {
      toShutDown = executor;
      executor = null;
    }
    if (toShutDown == null) {
      return;
    }
    toShutDown.shutdownNow();
    try {
      if (!toShutDown.awaitTermination(5, TimeUnit.SECONDS)) {
        LOGGER.warn("Casbin policy refresher did not stop within 5s");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  synchronized boolean isRunning() {
    return executor != null && !executor.isShutdown();
  }
}
