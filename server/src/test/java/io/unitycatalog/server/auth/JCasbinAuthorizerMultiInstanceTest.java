package io.unitycatalog.server.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for running the UC server as multiple instances against one shared database.
 *
 * <p>Every other authorizer test uses a single {@link JCasbinAuthorizer}, and jCasbin evaluates
 * every decision against an in-memory copy of the policy set that is loaded once in the
 * constructor. A single instance therefore always reads back its own writes, and the cross-instance
 * staleness bug is invisible by construction. These tests exist to close that gap.
 *
 * <p>Each {@code JCasbinAuthorizer} here stands in for one replica: they share the {@link
 * HibernateConfigurator} (and so the database), but each constructs its own {@code JDBCAdapter} and
 * its own {@code SyncedEnforcer}, exactly as separate pods would. {@code SyncedEnforcer} makes a
 * single instance thread-safe; it does not share policy state between instances, and its lock —
 * though static — cannot help here because the two instances hold two distinct {@code Model}s.
 *
 * <p>Propagation is eventually consistent, so the cross-instance assertions poll with a bounded
 * timeout rather than asserting immediately. Asserting immediately would be flaky by construction.
 */
public class JCasbinAuthorizerMultiInstanceTest {

  /** Generous enough to absorb a slow CI machine, short enough to fail fast when broken. */
  private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(5);

  /** Far shorter than production's default so the tests are not dominated by waiting. */
  private static final String TEST_REFRESH_INTERVAL = "PT0.05S";

  private HibernateConfigurator hibernateConfigurator;
  private final List<JCasbinAuthorizer> replicas = new ArrayList<>();

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.setProperty(Property.SERVER_ENV.getKey(), "test");
    hibernateConfigurator = new HibernateConfigurator(new ServerProperties(properties));
  }

  @AfterEach
  void tearDown() {
    // Close the replicas before the SessionFactory they poll through, and close them at all so the
    // refresh threads do not accumulate across the suite.
    replicas.forEach(JCasbinAuthorizer::close);
    replicas.clear();
    hibernateConfigurator.getSessionFactory().close();
  }

  private ServerProperties properties(boolean refreshEnabled, String interval) {
    Properties properties = new Properties();
    properties.setProperty(Property.SERVER_ENV.getKey(), "test");
    properties.setProperty(
        Property.POLICY_REFRESH_ENABLED.getKey(), refreshEnabled ? "enable" : "disable");
    properties.setProperty(Property.POLICY_REFRESH_INTERVAL.getKey(), interval);
    return new ServerProperties(properties);
  }

  /** Starts a replica that polls for changes, as a production instance does. */
  private JCasbinAuthorizer startReplica() throws Exception {
    return register(
        new JCasbinAuthorizer(hibernateConfigurator, properties(true, TEST_REFRESH_INTERVAL)));
  }

  /**
   * Starts a replica that does not poll, so a test can drive {@link
   * CasbinPolicyRefresher#checkAndReload()} itself and observe its return value rather than racing
   * the background thread.
   */
  private JCasbinAuthorizer startReplicaWithoutRefresh() throws Exception {
    return register(
        new JCasbinAuthorizer(hibernateConfigurator, properties(false, TEST_REFRESH_INTERVAL)));
  }

  private JCasbinAuthorizer register(JCasbinAuthorizer authorizer) {
    replicas.add(authorizer);
    return authorizer;
  }

  /**
   * Performs the one unconditional reload every refresher does on its first check, so that
   * subsequent assertions are about genuine changes. That first reload is deliberate — see {@link
   * CasbinPolicyRefresher} on why the version is not seeded in the constructor.
   */
  private static void settle(JCasbinAuthorizer replica) {
    assertThat(replica.getRefresher().checkAndReload())
        .as("the first check always reloads")
        .isTrue();
    assertThat(replica.getRefresher().checkAndReload())
        .as("nothing changed since the first check")
        .isFalse();
  }

  private static void await(String description, BooleanSupplier condition) {
    long deadline = System.nanoTime() + AWAIT_TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fail("Interrupted while waiting for " + description);
      }
    }
    fail("%s did not happen within %s", description, AWAIT_TIMEOUT);
  }

  /**
   * A grant issued through one replica must be honoured by every other replica.
   *
   * <p>Without session affinity the request that creates a resource and the request that next reads
   * it land on different pods, so the reader would otherwise deny access to a resource the writer
   * just created — a spurious {@code 403 PERMISSION_DENIED} that succeeds on retry.
   */
  @Test
  void grantOnOneReplicaIsVisibleToAnother() throws Exception {
    UnityCatalogAuthorizer replicaA = startReplica();
    UnityCatalogAuthorizer replicaB = startReplica();

    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();

    replicaA.grantAuthorization(principal, resource, Privileges.CREATE_CATALOG);

    assertThat(replicaA.authorize(principal, resource, Privileges.CREATE_CATALOG)).isTrue();
    await(
        "the grant to reach replica B",
        () -> replicaB.authorize(principal, resource, Privileges.CREATE_CATALOG));
  }

  /**
   * A revocation issued through one replica must be honoured by every other replica.
   *
   * <p>This is the security-relevant direction: a stale replica keeps <em>allowing</em> access, so
   * the request returns {@code 200 OK} and credentials are vended for a privilege that has already
   * been revoked. There is no error, and nothing in the server logs — only an audit surfaces it.
   *
   * <p>Replica B is started after the grant so that it begins with the privilege in its cache; the
   * assertion is specifically about the revocation propagating, not the grant.
   */
  @Test
  void revokeOnOneReplicaIsHonouredByAnother() throws Exception {
    UnityCatalogAuthorizer replicaA = startReplica();

    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    replicaA.grantAuthorization(principal, resource, Privileges.SELECT);

    UnityCatalogAuthorizer replicaB = startReplica();
    assertThat(replicaB.authorize(principal, resource, Privileges.SELECT)).isTrue();

    replicaA.revokeAuthorization(principal, resource, Privileges.SELECT);

    assertThat(replicaA.authorize(principal, resource, Privileges.SELECT)).isFalse();
    await(
        "the revocation to reach replica B",
        () -> !replicaB.authorize(principal, resource, Privileges.SELECT));
  }

  /**
   * A hierarchy edge added through one replica must be visible to every other replica.
   *
   * <p>Nested resources inherit non-OWNER privileges by walking the {@code g2} ancestor chain, so a
   * missing edge breaks inherited access on the child even when the grant on the parent is known.
   * The missing edge also makes {@code getHierarchyParent} return {@code null}, which {@code
   * PermissionService} treats as "no parent" rather than as an error — the parent and grandparent
   * owner checks are skipped and a permission listing silently degrades from every assignment to
   * only the caller's own.
   */
  @Test
  void hierarchyEdgeAddedOnOneReplicaIsVisibleToAnother() throws Exception {
    UnityCatalogAuthorizer replicaA = startReplica();
    UnityCatalogAuthorizer replicaB = startReplica();

    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();

    replicaA.addHierarchyChild(catalog, schema);
    replicaA.grantAuthorization(principal, catalog, Privileges.SELECT);

    await(
        "the hierarchy edge to reach replica B",
        () -> catalog.equals(replicaB.getHierarchyParent(schema)));
    await(
        "the inherited privilege to reach replica B",
        () -> replicaB.authorize(principal, schema, Privileges.SELECT));
  }

  /**
   * Control test: the write itself reaches the shared database.
   *
   * <p>A replica started after the grant sees it immediately, which localises any propagation
   * failure to the in-memory cache of already-running replicas rather than to the persistence path.
   * This is also why a pod restart appeared to "fix" the problem in production while nothing had
   * actually been repaired — the restart only reloaded the cache.
   */
  @Test
  void grantIsPersistedAndVisibleToAReplicaStartedAfterwards() throws Exception {
    UnityCatalogAuthorizer replicaA = startReplica();

    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    replicaA.grantAuthorization(principal, resource, Privileges.CREATE_CATALOG);

    UnityCatalogAuthorizer replicaStartedLater = startReplica();

    assertThat(replicaStartedLater.authorize(principal, resource, Privileges.CREATE_CATALOG))
        .isTrue();
  }

  /**
   * Change detection must notice an insert.
   *
   * <p>Detection compares {@code (count(*), max(id))} on {@code casbin_rule}. An insert always
   * moves {@code max(id)}, because the adapter declares {@code id} as auto-incrementing on every
   * dialect.
   */
  @Test
  void detectsAnInsert() throws Exception {
    UnityCatalogAuthorizer writer = startReplicaWithoutRefresh();
    JCasbinAuthorizer reader = startReplicaWithoutRefresh();
    settle(reader);

    writer.grantAuthorization(UUID.randomUUID(), UUID.randomUUID(), Privileges.SELECT);

    assertThat(reader.getRefresher().checkAndReload()).isTrue();
  }

  /**
   * Change detection must notice a delete.
   *
   * <p>A delete leaves {@code max(id)} alone but lowers {@code count(*)}, which is the only reason
   * the count is part of the pair at all.
   */
  @Test
  void detectsADelete() throws Exception {
    UnityCatalogAuthorizer writer = startReplicaWithoutRefresh();
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    writer.grantAuthorization(principal, resource, Privileges.SELECT);

    JCasbinAuthorizer reader = startReplicaWithoutRefresh();
    settle(reader);

    writer.revokeAuthorization(principal, resource, Privileges.SELECT);

    assertThat(reader.getRefresher().checkAndReload()).isTrue();
  }

  /**
   * Change detection must notice a delete paired with an insert, the case that defeats a count on
   * its own: the count returns to where it started, and only {@code max(id)} reveals the change.
   */
  @Test
  void detectsADeleteAndInsertThatLeavesTheCountUnchanged() throws Exception {
    UnityCatalogAuthorizer writer = startReplicaWithoutRefresh();
    UUID principal = UUID.randomUUID();
    UUID revoked = UUID.randomUUID();
    writer.grantAuthorization(principal, revoked, Privileges.SELECT);

    JCasbinAuthorizer reader = startReplicaWithoutRefresh();
    settle(reader);

    // One row out, one row in: same count, higher max id.
    writer.revokeAuthorization(principal, revoked, Privileges.SELECT);
    UUID granted = UUID.randomUUID();
    writer.grantAuthorization(principal, granted, Privileges.SELECT);

    assertThat(reader.getRefresher().checkAndReload()).isTrue();
    assertThat(reader.authorize(principal, granted, Privileges.SELECT)).isTrue();
    assertThat(reader.authorize(principal, revoked, Privileges.SELECT)).isFalse();
  }

  /**
   * The deny-triggered refresh must be rate-limited. It runs on a path reachable by unauthenticated
   * callers, so without a debounce a client hammering a denied endpoint would force continuous
   * full-table reloads, each holding the enforcer's write lock.
   */
  @Test
  void denyTriggeredRefreshIsDebounced() throws Exception {
    JCasbinAuthorizer replica = startReplica();

    // Default debounce is 1s, so only the first of two immediate calls may do any work. The
    // background poll uses checkAndReload rather than this path, so it cannot perturb the window.
    assertThat(replica.refreshAuthorizations()).isTrue();
    assertThat(replica.refreshAuthorizations()).isFalse();
  }

  /**
   * Disabling refresh must disable all of it, including the deny-triggered reload — otherwise the
   * setting is not a kill switch and an operator cannot fully roll back to the previous behaviour.
   */
  @Test
  void disablingRefreshAlsoDisablesTheDenyTriggeredReload() throws Exception {
    JCasbinAuthorizer replica = startReplicaWithoutRefresh();

    assertThat(replica.getRefresher().isRunning()).isFalse();
    assertThat(replica.refreshAuthorizations()).isFalse();
  }

  /**
   * With refresh disabled, a replica must exhibit the original stale behaviour. This is what makes
   * the enabled cases above meaningful: it shows the polling is what fixes them, rather than
   * something incidental to the test setup.
   */
  @Test
  void withRefreshDisabledAGrantNeverReachesAnotherReplica() throws Exception {
    UnityCatalogAuthorizer replicaA = startReplicaWithoutRefresh();
    UnityCatalogAuthorizer replicaB = startReplicaWithoutRefresh();

    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();

    replicaA.grantAuthorization(principal, resource, Privileges.CREATE_CATALOG);

    assertThat(replicaA.authorize(principal, resource, Privileges.CREATE_CATALOG)).isTrue();
    assertThat(replicaB.authorize(principal, resource, Privileges.CREATE_CATALOG)).isFalse();
  }

  /** A closed authorizer must not leave its refresh thread behind. */
  @Test
  void closeStopsTheRefreshThread() throws Exception {
    JCasbinAuthorizer replica = startReplica();
    assertThat(replica.getRefresher().isRunning()).isTrue();

    replica.close();

    assertThat(replica.getRefresher().isRunning()).isFalse();
  }
}
