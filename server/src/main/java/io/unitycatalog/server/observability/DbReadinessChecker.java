package io.unitycatalog.server.observability;

import com.linecorp.armeria.server.healthcheck.ListenableHealthChecker;
import com.linecorp.armeria.server.healthcheck.SettableHealthChecker;
import java.time.Duration;
import java.util.Objects;
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

  /** How long {@link #close()} waits for an in-flight probe to finish before returning. */
  private static final long SHUTDOWN_AWAIT_SECONDS = 2;

  /** A single reachability check against the database. */
  @FunctionalInterface
  interface DbProbe {
    boolean isReachable() throws Exception;
  }

  private final DbProbe probe;
  private final Duration interval;
  // Starts not-ready: /readyz returns 503 until the first probe confirms DB is reachable.
  private final SettableHealthChecker health = new SettableHealthChecker(false);
  private ScheduledExecutorService scheduler;
  // Set while shutting down so the interrupt from close() is not logged as a probe failure.
  private volatile boolean closing;

  DbReadinessChecker(DbProbe probe, Duration interval) {
    this.probe = Objects.requireNonNull(probe, "probe");
    this.interval = interval;
  }

  /**
   * Production factory: probes via {@code Connection.isValid} on a short-lived session. {@code
   * dbTimeout} bounds only the validity check, not connection acquisition: if the DB is
   * unreachable, opening the session can block on the driver/pool's connect timeout — including the
   * synchronous probe {@link #start()} runs on the startup thread. Configure the connection pool's
   * connect timeout to bound that.
   */
  public static DbReadinessChecker forSessionFactory(
      SessionFactory sessionFactory, Duration interval, Duration dbTimeout) {
    // Connection.isValid takes whole seconds; treat any positive sub-second timeout as 1s.
    int timeoutSeconds = (int) Math.max(1, dbTimeout.toSeconds());
    DbProbe probe =
        () -> {
          try (var session = sessionFactory.openSession()) {
            return session.doReturningWork(conn -> conn.isValid(timeoutSeconds));
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
    } catch (Throwable t) {
      // Catch Throwable, not just Exception: an Error escaping a scheduleAtFixedRate task would
      // silently cancel all future runs and freeze readiness. Fail closed on anything.
      if (!closing) {
        LOG.warn("Readiness DB probe failed; marking not-ready", t);
      }
      healthy = false;
    }
    health.setHealthy(healthy);
  }

  /**
   * Synchronous initial probe (on the calling/startup thread), then periodic background refresh.
   */
  public synchronized void start() {
    if (scheduler != null) {
      return;
    }
    closing = false;
    refresh();
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "db-readiness-checker");
              t.setDaemon(true);
              return t;
            });
    scheduler.scheduleAtFixedRate(
        this::refresh, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized void close() {
    closing = true;
    if (scheduler == null) {
      return;
    }
    scheduler.shutdownNow();
    try {
      // Wait briefly so an in-flight probe finishes before the caller closes the SessionFactory.
      if (!scheduler.awaitTermination(SHUTDOWN_AWAIT_SECONDS, TimeUnit.SECONDS)) {
        LOG.warn("Readiness probe did not stop within {}s of shutdown", SHUTDOWN_AWAIT_SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      scheduler = null;
    }
  }
}
