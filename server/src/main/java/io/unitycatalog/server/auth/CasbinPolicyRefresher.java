package io.unitycatalog.server.auth;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
 * <p>Two probes, for different use cases:
 *
 * <ul>
 *   <li>The background poller uses {@link #VERSION_QUERY} ({@code count(*)} and {@code max(id)}) so
 *       a revoke that deletes a non-max row still shows up.
 *   <li>The deny path uses {@link #MAX_ID_QUERY} ({@code max(id)} only). A stale deny after a grant
 *       is a missing insert; revokes are left to the poller.
 * </ul>
 *
 * <p>Reload is delegated to {@link JCasbinAuthorizer} so a fresh enforcer can be swapped without
 * blocking {@code enforce()}.
 */
public class CasbinPolicyRefresher implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CasbinPolicyRefresher.class);

  private enum ProbeResult {
    FAILED,
    UNCHANGED,
    RELOADED
  }

  // Poller: count(*) catches revokes that delete a non-max row; max(id) catches new grants.
  private static final String VERSION_QUERY =
      "select count(*), coalesce(max(id), 0) from casbin_rule";
  // Deny path: a stale deny after a grant is a missing insert, which always raises max(id).
  private static final String MAX_ID_QUERY = "select coalesce(max(id), 0) from casbin_rule";

  private final Runnable reloader;
  private final SessionFactory sessionFactory;
  private final Duration minProbeInterval;

  // Initial -1 forces a reload on first check (see recordVersion).
  private long lastCount = -1;
  private long lastMaxId = -1;

  // System.nanoTime() of the last successful probe start; Long.MIN_VALUE if none.
  private volatile long lastCompletedProbeAtNanos = Long.MIN_VALUE;

  private ScheduledExecutorService executor;

  public CasbinPolicyRefresher(
      Runnable reloader, SessionFactory sessionFactory, Duration minProbeInterval) {
    this.reloader = reloader;
    this.sessionFactory = sessionFactory;
    this.minProbeInterval = minProbeInterval == null ? Duration.ZERO : minProbeInterval;
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
    LOGGER.info(
        "Casbin policy refresh enabled, checking every {}ms; deny-path min probe interval {}ms",
        millis,
        minProbeInterval.toMillis());
  }

  private void pollQuietly() {
    try {
      checkAndReload();
    } catch (Throwable t) {
      LOGGER.warn("Casbin policy refresh check failed; will retry on the next interval", t);
    }
  }

  /**
   * Poller probe: {@link #VERSION_QUERY} ({@code count(*)} and {@code max(id)}).
   *
   * @return true if a reload happened
   */
  public boolean checkAndReload() {
    synchronized (this) {
      return probeCountAndMaxId() == ProbeResult.RELOADED;
    }
  }

  /**
   * Reloads if policy may have changed after {@code operationStartNanos} ({@link System#nanoTime()}
   * from the start of the denied operation).
   */
  public boolean checkAndReloadAfter(long operationStartNanos) {
    if (isCoveredByCompletedProbe(operationStartNanos)) {
      return true;
    }
    synchronized (this) {
      while (true) {
        if (isCoveredByCompletedProbe(operationStartNanos)) {
          return true;
        }
        Duration remaining = remainingMinProbeInterval();
        if (!remaining.isZero()) {
          if (!await(remaining)) {
            return false;
          }
          continue;
        }
        return probeMaxId() != ProbeResult.FAILED;
      }
    }
  }

  private boolean isCoveredByCompletedProbe(long operationStartNanos) {
    long lastStartNanos = lastCompletedProbeAtNanos;
    return lastStartNanos != Long.MIN_VALUE && lastStartNanos - operationStartNanos >= 0;
  }

  private Duration remainingMinProbeInterval() {
    long lastStartNanos = lastCompletedProbeAtNanos;
    if (minProbeInterval.isZero() || lastStartNanos == Long.MIN_VALUE) {
      return Duration.ZERO;
    }
    Duration remaining =
        minProbeInterval.minus(Duration.ofNanos(System.nanoTime() - lastStartNanos));
    return remaining.isNegative() ? Duration.ZERO : remaining;
  }

  private boolean await(Duration remaining) {
    try {
      wait(Math.max(1, remaining.toMillis()));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private ProbeResult probeCountAndMaxId() {
    long probeStartedNanos = System.nanoTime();
    long[] db = readVersion().orElse(null);
    if (db == null) {
      return ProbeResult.FAILED;
    }
    if (db[0] == lastCount && db[1] == lastMaxId) {
      recordCompletedProbe(probeStartedNanos);
      return ProbeResult.UNCHANGED;
    }
    return reload(probeStartedNanos, db[0], db[1]);
  }

  private ProbeResult probeMaxId() {
    long probeStartedNanos = System.nanoTime();
    Long maxId = readMaxId().orElse(null);
    if (maxId == null) {
      return ProbeResult.FAILED;
    }
    if (maxId == lastMaxId) {
      recordCompletedProbe(probeStartedNanos);
      return ProbeResult.UNCHANGED;
    }
    return reload(probeStartedNanos, lastCount, maxId);
  }

  private ProbeResult reload(long probeStartedNanos, long count, long maxId) {
    long previousCount = lastCount;
    long previousMaxId = lastMaxId;
    reloader.run();
    recordVersion(count, maxId);
    recordCompletedProbe(probeStartedNanos);
    LOGGER.info(
        "Reloaded Casbin policy from casbin_rule (count {} -> {}, maxId {} -> {})",
        previousCount,
        count,
        previousMaxId,
        maxId);
    return ProbeResult.RELOADED;
  }

  private void recordCompletedProbe(long probeStartedNanos) {
    lastCompletedProbeAtNanos = probeStartedNanos;
    notifyAll();
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

  private Optional<Long> readMaxId() {
    try {
      return sessionFactory.fromSession(
          session -> {
            NativeQuery<?> query = session.createNativeQuery(MAX_ID_QUERY);
            Object result = query.getSingleResult();
            return Optional.of(((Number) result).longValue());
          });
    } catch (Exception e) {
      LOGGER.debug("Could not read the Casbin policy max id", e);
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
