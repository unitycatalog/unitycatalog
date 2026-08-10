package io.unitycatalog.server.auth;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.casbin.jcasbin.main.SyncedEnforcer;
import org.hibernate.SessionFactory;
import org.hibernate.query.NativeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reloads the in-memory Casbin policy when {@code casbin_rule} changes in the shared database.
 *
 * <p>Change detection uses {@code select count(*), coalesce(max(id), 0) from casbin_rule}. The JDBC
 * adapter only inserts and deletes rows with auto-incrementing ids, so any write changes at least one
 * of the two values. Requires a {@link SyncedEnforcer} so {@code loadPolicy()} is safe under concurrent
 * {@code enforce()} calls.
 */
public class CasbinPolicyRefresher implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CasbinPolicyRefresher.class);

  private static final String VERSION_QUERY =
      "select count(*), coalesce(max(id), 0) from casbin_rule";

  private final SyncedEnforcer enforcer;
  private final SessionFactory sessionFactory;

  // Initial -1 forces a reload on first check (see recordVersion).
  private long lastCount = -1;
  private long lastMaxId = -1;

  private ScheduledExecutorService executor;

  public CasbinPolicyRefresher(SyncedEnforcer enforcer, SessionFactory sessionFactory) {
    this.enforcer = enforcer;
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

  /** @return true if a reload happened */
  public synchronized boolean checkAndReload() {
    long[] version = readVersion().orElse(null);
    if (version == null) {
      return false;
    }
    if (version[0] == lastCount && version[1] == lastMaxId) {
      return false;
    }
    LOGGER.debug(
        "Casbin policy changed (count {} -> {}, maxId {} -> {}); reloading",
        lastCount,
        version[0],
        lastMaxId,
        version[1]);
    enforcer.loadPolicy();
    recordVersion(version[0], version[1]);
    return true;
  }

  /** Unconditional reload for callers that cannot wait for the next poll. */
  public synchronized void forceReload() {
    enforcer.loadPolicy();
    readVersion().ifPresent(version -> recordVersion(version[0], version[1]));
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
