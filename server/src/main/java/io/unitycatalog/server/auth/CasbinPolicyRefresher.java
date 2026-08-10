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
 * Keeps one server instance's in-memory Casbin policy set in step with the shared database.
 *
 * <p>jCasbin evaluates every decision against an in-memory copy of the policy set that {@link
 * org.casbin.jcasbin.main.CoreEnforcer} loads once, in its constructor. Writes go to the writing
 * instance's memory and to {@code casbin_rule}, but no other instance ever learns about them. With
 * more than one replica and no session affinity that means a grant made through one replica is
 * invisible to the others, and — more seriously — a revocation made through one replica is not
 * honoured by the others, which keep allowing access and vending credentials with no error and
 * nothing in the log. This class closes that gap by reloading when the table changes.
 *
 * <h2>Change detection</h2>
 *
 * <p>Detection is {@code select count(*), coalesce(max(id), 0) from casbin_rule}: when either value
 * differs from the previously observed pair, the policy set is reloaded. That pair cannot miss a
 * change, given two properties of the casbin JDBC adapter:
 *
 * <ul>
 *   <li>{@code casbin_rule.id} is an auto-incrementing primary key in every dialect the adapter
 *       supports, so an insert always moves {@code max(id)}.
 *   <li>The adapter only ever inserts and deletes — it issues no {@code UPDATE} against the table,
 *       and even {@code updatePolicy} is a delete followed by an insert.
 * </ul>
 *
 * <p>So an insert moves {@code max(id)}, a delete moves {@code count(*)}, and a delete paired with
 * an insert leaves the count unchanged but still moves {@code max(id)}. No in-place update exists
 * that could alter a row's contents without moving one of the two. The comparison is for inequality
 * rather than for growth, so even a sequence reset triggers a reload.
 *
 * <p>Deliberately plain standard SQL: it works on every backend the adapter supports and needs no
 * schema change, no extra table, and no database-specific notification mechanism.
 *
 * <h2>Concurrency</h2>
 *
 * <p>Reloading is safe to do while requests are being served, but only because the enforcer is a
 * {@link SyncedEnforcer}: it overrides {@code loadPolicy()} to hold a write lock for the whole
 * clear-then-repopulate cycle, while {@code enforce()} holds the matching read lock. A plain {@code
 * Enforcer} would let concurrent {@code enforce()} calls observe the momentarily empty model that
 * {@code loadPolicy()} passes through, denying valid requests.
 */
public class CasbinPolicyRefresher implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CasbinPolicyRefresher.class);

  /**
   * Reuses Hibernate's connection pool rather than opening a second one. {@code casbin_rule} is not
   * a Hibernate-managed entity, so this has to be a native query.
   */
  private static final String VERSION_QUERY =
      "select count(*), coalesce(max(id), 0) from casbin_rule";

  private final SyncedEnforcer enforcer;
  private final SessionFactory sessionFactory;

  /**
   * Last observed {@code (count, maxId)}. Guarded by {@code this} along with every reload, so the
   * scheduled poll and a deny-triggered refresh can never interleave.
   *
   * <p>Deliberately left at a value the table can never report, so the first check always reloads.
   * Seeding this from the table in the constructor would be a race: the enforcer loads the policy
   * set before the refresher is built, so a write landing between the two would already be included
   * in the seeded version while never having reached the enforcer — and would then never be picked
   * up. One redundant reload at startup is a cheap price for closing that.
   */
  private long lastCount = -1;

  private long lastMaxId = -1;

  private ScheduledExecutorService executor;

  public CasbinPolicyRefresher(SyncedEnforcer enforcer, SessionFactory sessionFactory) {
    this.enforcer = enforcer;
    this.sessionFactory = sessionFactory;
  }

  /**
   * Starts polling for changes made by other instances.
   *
   * @param interval delay between the end of one check and the start of the next
   */
  public synchronized void start(Duration interval) {
    if (executor != null) {
      return;
    }
    executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "casbin-policy-refresher");
              // Daemon so a forgotten refresher can never hold the JVM open.
              thread.setDaemon(true);
              return thread;
            });
    long millis = Math.max(1, interval.toMillis());
    // Fixed delay rather than fixed rate: a reload slower than the interval must not cause runs to
    // queue up back to back.
    executor.scheduleWithFixedDelay(this::pollQuietly, millis, millis, TimeUnit.MILLISECONDS);
    LOGGER.info("Casbin policy refresh enabled, checking every {}ms", millis);
  }

  /** Never lets a failure escape into the executor, which would silently cancel the schedule. */
  private void pollQuietly() {
    try {
      checkAndReload();
    } catch (Throwable t) {
      LOGGER.warn("Casbin policy refresh check failed; will retry on the next interval", t);
    }
  }

  /**
   * Reloads the policy set if the table has changed since the last observation.
   *
   * @return true if a reload happened
   */
  public synchronized boolean checkAndReload() {
    long[] version = readVersion().orElse(null);
    if (version == null) {
      // Could not read the table. Leave the last observed values alone so the next check compares
      // against a real observation rather than treating the failure as a change.
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

  /**
   * Reloads unconditionally, for callers that cannot wait for the next poll — a request about to be
   * denied, where the grant may have been made through another replica moments ago.
   */
  public synchronized void forceReload() {
    enforcer.loadPolicy();
    readVersion().ifPresent(version -> recordVersion(version[0], version[1]));
  }

  private void recordVersion(long count, long maxId) {
    lastCount = count;
    lastMaxId = maxId;
  }

  /**
   * Reads {@code (count, maxId)}, or empty when the table cannot be read — expected before the
   * adapter has created it, and possible during a transient database failure.
   */
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

  /** Stops polling. Idempotent, and safe to call whether or not {@link #start} ever ran. */
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

  /** Visible for testing: whether the polling thread is currently scheduled. */
  synchronized boolean isRunning() {
    return executor != null && !executor.isShutdown();
  }
}
