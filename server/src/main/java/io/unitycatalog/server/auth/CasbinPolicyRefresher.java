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
 * <p>The poller uses {@code count(*)} and {@code max(id)}; deny-path checks use {@code max(id)}
 * only. Reload is delegated to {@link JCasbinAuthorizer} so a fresh enforcer can be swapped without
 * blocking {@code enforce()}.
 */
public class CasbinPolicyRefresher implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CasbinPolicyRefresher.class);

  private enum ProbeResult {
    FAILED,
    UNCHANGED,
    RELOADED
  }

  private static final String VERSION_QUERY =
      "select count(*), coalesce(max(id), 0) from casbin_rule";
  private static final String MAX_ID_QUERY = "select coalesce(max(id), 0) from casbin_rule";

  private final Runnable reloader;
  private final SessionFactory sessionFactory;
  private final Duration minProbeInterval;

  // Initial -1 forces a reload on first check (see recordVersion).
  private long lastCount = -1;
  private long lastMaxId = -1;

  // System.nanoTime() of the last successful probe start; Long.MIN_VALUE if none.
  private volatile long lastCompletedProbeAt = Long.MIN_VALUE;

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
   * Probes {@code casbin_rule} and reloads when the version changed.
   *
   * @return true if a reload happened
   */
  public boolean checkAndReload() {
    synchronized (this) {
      return probeAndReload(true) == ProbeResult.RELOADED;
    }
  }

  /**
   * Reloads if policy may have changed after {@code observedBefore} ({@link System#nanoTime()}).
   */
  public boolean checkAndReloadAfter(long observedBefore) {
    if (isCoveredByCompletedProbe(observedBefore)) {
      return true;
    }
    synchronized (this) {
      while (true) {
        if (isCoveredByCompletedProbe(observedBefore)) {
          return true;
        }
        Duration remaining = remainingMinProbeInterval();
        if (!remaining.isZero()) {
          if (!await(remaining)) {
            return false;
          }
          continue;
        }
        return probeAndReload(false) != ProbeResult.FAILED;
      }
    }
  }

  private boolean isCoveredByCompletedProbe(long observedBefore) {
    long lastStart = lastCompletedProbeAt;
    return lastStart != Long.MIN_VALUE && lastStart - observedBefore >= 0;
  }

  private Duration remainingMinProbeInterval() {
    long lastStart = lastCompletedProbeAt;
    if (minProbeInterval.isZero() || lastStart == Long.MIN_VALUE) {
      return Duration.ZERO;
    }
    Duration remaining = minProbeInterval.minus(Duration.ofNanos(System.nanoTime() - lastStart));
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

  private ProbeResult probeAndReload(boolean fullVersion) {
    // Query snapshot is taken at start, so this instant is what the probe can cover.
    long probeStarted = System.nanoTime();
    if (fullVersion) {
      long[] db = readVersion().orElse(null);
      if (db == null) {
        return ProbeResult.FAILED;
      }
      if (db[0] == lastCount && db[1] == lastMaxId) {
        recordCompletedProbe(probeStarted);
        return ProbeResult.UNCHANGED;
      }
      return reload(probeStarted, db[0], db[1]);
    }

    Long maxId = readMaxId().orElse(null);
    if (maxId == null) {
      return ProbeResult.FAILED;
    }
    if (maxId == lastMaxId) {
      recordCompletedProbe(probeStarted);
      return ProbeResult.UNCHANGED;
    }
    return reload(probeStarted, lastCount, maxId);
  }

  private ProbeResult reload(long probeStarted, long count, long maxId) {
    long previousCount = lastCount;
    long previousMaxId = lastMaxId;
    reloader.run();
    recordVersion(count, maxId);
    recordCompletedProbe(probeStarted);
    LOGGER.info(
        "Reloaded Casbin policy from casbin_rule (count {} -> {}, maxId {} -> {})",
        previousCount,
        count,
        previousMaxId,
        maxId);
    return ProbeResult.RELOADED;
  }

  private void recordCompletedProbe(long probeStarted) {
    lastCompletedProbeAt = probeStarted;
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
