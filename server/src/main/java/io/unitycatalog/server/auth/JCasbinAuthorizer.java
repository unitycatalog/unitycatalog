package io.unitycatalog.server.auth;

import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.casbin.adapter.JDBCAdapter;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.main.SyncedEnforcer;
import org.casbin.jcasbin.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authorizer that uses the JCasbin library to enforce access control policies.
 *
 * <p>This class is an implementation of UnityCatalogAuthorizor that uses JCasbin as the back end to
 * both store and enforce access control policies.
 *
 * <p>The implementation stores the policies in a database using the JDBCAdapter class. A {@link
 * SyncedEnforcer} is used because the UC server shares one authorizer across concurrent REST
 * requests; jCasbin's plain {@link Enforcer} is not thread-safe for concurrent {@code enforce()}
 * and policy mutations.
 *
 * <p>The policy set lives in memory and is loaded once by the enforcer's constructor, so an
 * instance would otherwise never see a grant or revocation made through another instance. A {@link
 * CasbinPolicyRefresher} polls the shared table and reloads on change, which is what makes running
 * more than one server instance against one database safe.
 */
public class JCasbinAuthorizer implements UnityCatalogAuthorizer, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(JCasbinAuthorizer.class);

  private final SyncedEnforcer enforcer;
  private final CasbinPolicyRefresher refresher;
  private final long refreshDebounceNanos;

  /**
   * Gates every part of cross-instance policy refresh — both the background poll and the
   * deny-triggered reload — so that disabling it restores exactly the behaviour from before refresh
   * existed. A flag that only turned off the poll would not be a usable kill switch.
   */
  private final boolean refreshEnabled;

  /**
   * When the last deny-triggered refresh was claimed. Seeded so the very first deny is allowed to
   * refresh rather than being treated as inside a window that never happened.
   */
  private final AtomicLong lastRefreshNanos = new AtomicLong(Long.MIN_VALUE / 2);

  private static final int PRINCIPAL_INDEX = 0;
  private static final int RESOURCE_INDEX = 1;
  private static final int PRIVILEGE_INDEX = 2;

  private static final String HIERARCHY_POLICY = "g2";
  private static final int HIERARCHY_PARENT_INDEX = 0;
  private static final int HIERARCHY_CHILD_INDEX = 1;

  public JCasbinAuthorizer(
      HibernateConfigurator hibernateConfigurator, ServerProperties serverProperties)
      throws Exception {
    Properties properties = hibernateConfigurator.getHibernateProperties();
    String driver = properties.getProperty("hibernate.connection.driver_class");
    String url = properties.getProperty("hibernate.connection.url");
    String user = resolveConnectionUsername(properties);
    String password = properties.getProperty("hibernate.connection.password");
    JDBCAdapter adapter = new JDBCAdapter(driver, url, user, password);

    InputStream modelStream = this.getClass().getResourceAsStream("/jcasbin_auth_model.conf");
    String string = IOUtils.toString(modelStream, StandardCharsets.UTF_8);
    Model model = new Model();
    model.loadModelFromText(string);

    enforcer = new SyncedEnforcer(model, adapter);
    enforcer.enableAutoSave(true);

    this.refreshDebounceNanos = serverProperties.getPolicyRefreshDebounce().toNanos();
    this.refreshEnabled = serverProperties.isPolicyRefreshEnabled();
    this.refresher = new CasbinPolicyRefresher(enforcer, hibernateConfigurator.getSessionFactory());
    if (refreshEnabled) {
      refresher.start(serverProperties.getPolicyRefreshInterval());
    } else {
      LOGGER.warn(
          "Casbin policy refresh is disabled. Authorization changes made through another server"
              + " instance will not be seen by this one, so running more than one instance against"
              + " this database is unsafe.");
    }
  }

  /**
   * Resolves the database connection username from the Hibernate properties.
   *
   * <p>Prefers the standard Hibernate key {@code hibernate.connection.username} (used by the main
   * session factory configuration and by the project's own tests) and falls back to the
   * non-standard {@code hibernate.connection.user} that the deployment docs and Helm chart
   * document. Reading only {@code hibernate.connection.user} left the casbin JDBC adapter with a
   * null username for any standard configuration, which the JDBC driver then silently replaced with
   * a process default.
   */
  static String resolveConnectionUsername(Properties properties) {
    String username = properties.getProperty("hibernate.connection.username");
    if (username == null) {
      username = properties.getProperty("hibernate.connection.user");
    }
    return username;
  }

  @Override
  public boolean grantAuthorization(UUID principal, UUID resource, Privileges action) {
    return enforcer.addPolicy(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean revokeAuthorization(UUID principal, UUID resource, Privileges action) {
    return enforcer.removePolicy(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean clearAuthorizationsForPrincipal(UUID principal) {
    return enforcer.removeFilteredPolicy(PRINCIPAL_INDEX, principal.toString());
  }

  @Override
  public boolean clearAuthorizationsForResource(UUID resource) {
    return enforcer.removeFilteredPolicy(RESOURCE_INDEX, resource.toString());
  }

  @Override
  public boolean addHierarchyChild(UUID parent, UUID child) {
    return enforcer.addNamedGroupingPolicy(HIERARCHY_POLICY, parent.toString(), child.toString());
  }

  @Override
  public boolean removeHierarchyChild(UUID parent, UUID child) {
    return enforcer.removeNamedGroupingPolicy(
        HIERARCHY_POLICY, parent.toString(), child.toString());
  }

  @Override
  public boolean removeHierarchyChildren(UUID resource) {
    return enforcer.removeFilteredNamedGroupingPolicy(
        HIERARCHY_POLICY, HIERARCHY_PARENT_INDEX, resource.toString());
  }

  @Override
  public UUID getHierarchyParent(UUID resource) {
    List<List<String>> policy =
        enforcer.getFilteredNamedGroupingPolicy(
            HIERARCHY_POLICY, HIERARCHY_CHILD_INDEX, resource.toString());
    if (policy.isEmpty() || policy.get(0).isEmpty()) {
      return null;
    }
    return UUID.fromString(policy.get(0).get(HIERARCHY_PARENT_INDEX));
  }

  @Override
  public boolean authorize(UUID principal, UUID resource, Privileges action) {
    return enforcer.enforce(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean authorizeAny(UUID principal, UUID resource, Privileges... actions) {
    return Arrays.stream(actions)
        .anyMatch(
            action ->
                enforcer.enforce(principal.toString(), resource.toString(), action.toString()));
  }

  @Override
  public boolean authorizeAll(UUID principal, UUID resource, Privileges... actions) {
    return Arrays.stream(actions)
        .allMatch(
            action ->
                enforcer.enforce(principal.toString(), resource.toString(), action.toString()));
  }

  @Override
  public List<Privileges> listAuthorizations(UUID principal, UUID resource) {
    List<List<String>> list =
        enforcer.getPermissionsForUserInDomain(principal.toString(), resource.toString());
    return list.stream()
        .map(l -> l.get(PRIVILEGE_INDEX))
        .map(Privileges::fromValue)
        .collect(Collectors.toList());
  }

  @Override
  public Map<UUID, List<Privileges>> listAuthorizations(UUID resource) {
    return enforcer.getFilteredPolicy(RESOURCE_INDEX, resource.toString()).stream()
        .collect(
            Collectors.groupingBy(
                l -> UUID.fromString(l.get(PRINCIPAL_INDEX)),
                Collectors.mapping(
                    l -> Privileges.fromValue(l.get(PRIVILEGE_INDEX)), Collectors.toList())));
  }

  /**
   * Reloads the policy set so a request denied against a stale view can be re-evaluated, at most
   * once per configured debounce interval.
   *
   * <p>The timestamp is claimed <em>before</em> reloading, and losing the compare-and-set means
   * returning false rather than waiting. Both matter: this runs on a path reachable by
   * unauthenticated callers, so without them a client repeatedly hitting a denied endpoint could
   * force continuous full-table reloads, each taking the enforcer's write lock and stalling every
   * concurrent authorization check.
   */
  @Override
  public boolean refreshAuthorizations() {
    if (!refreshEnabled) {
      return false;
    }
    long now = System.nanoTime();
    long last = lastRefreshNanos.get();
    if (now - last < refreshDebounceNanos) {
      return false;
    }
    if (!lastRefreshNanos.compareAndSet(last, now)) {
      return false;
    }
    refresher.forceReload();
    return true;
  }

  /** Stops the background policy refresh. The enforcer and its adapter need no explicit cleanup. */
  @Override
  public void close() {
    refresher.close();
  }

  /** Visible for testing. */
  CasbinPolicyRefresher getRefresher() {
    return refresher;
  }
}
