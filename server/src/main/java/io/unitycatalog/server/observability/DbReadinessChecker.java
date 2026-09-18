package io.unitycatalog.server.observability;

import com.linecorp.armeria.server.healthcheck.ListenableHealthChecker;
import com.linecorp.armeria.server.healthcheck.SettableHealthChecker;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks whether the backing database is reachable, for the {@code /readyz} probe. The reachability
 * check runs on a background scheduler (never on the request path); the probe endpoint only reads
 * the cached state. {@link #start()} runs one synchronous probe before scheduling, so readiness is
 * accurate the moment the server begins serving.
 */
public class DbReadinessChecker implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DbReadinessChecker.class);

  /** A single reachability check against the database. */
  @FunctionalInterface
  public interface DbProbe {
    boolean isReachable() throws Exception;
  }

  private final DbProbe probe;
  private final Duration interval;
  // Starts not-ready: /readyz returns 503 until the first probe confirms DB is reachable.
  private final SettableHealthChecker health = new SettableHealthChecker(false);
  private ScheduledExecutorService executor;

  public DbReadinessChecker(DbProbe probe, Duration interval) {
    this.probe = probe;
    this.interval = interval;
  }

  /** Production factory: probes via {@code Connection.isValid(2s)} on a short-lived session. */
  public static DbReadinessChecker forSessionFactory(
      SessionFactory sessionFactory, Duration interval) {
    DbProbe probe =
        () -> {
          try (var session = sessionFactory.openSession()) {
            return session.doReturningWork(conn -> conn.isValid(2));
          }
        };
    return new DbReadinessChecker(probe, interval);
  }

  public ListenableHealthChecker healthChecker() {
    return health;
  }

  /** Runs one probe now and records the result. Package-visible for deterministic tests. */
  void refresh() {
    boolean healthy;
    try {
      healthy = probe.isReachable();
    } catch (Exception e) {
      LOG.warn("Readiness DB probe failed; marking not-ready", e);
      healthy = false;
    }
    health.setHealthy(healthy);
  }

  /**
   * Synchronous initial probe (on the calling/startup thread), then periodic background refresh.
   */
  public synchronized void start() {
    if (executor != null) {
      return;
    }
    refresh();
    executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "db-readiness-checker");
              t.setDaemon(true);
              return t;
            });
    executor.scheduleAtFixedRate(
        this::refresh, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized void close() {
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
    }
  }
}
