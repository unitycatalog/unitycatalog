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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
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
 * <p>{@link CasbinPolicyRefresher} polls the shared {@code casbin_rule} table so grants and
 * revocations made through other instances are picked up. Reload builds a fresh enforcer and swaps
 * it under the exclusive side of {@code reloadLock} so {@code enforce()} keeps using the previous
 * instance. Local writes take the shared side so they are not lost across the swap, but do not wait
 * for each other.
 */
public class JCasbinAuthorizer implements UnityCatalogAuthorizer, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(JCasbinAuthorizer.class);

  private final AtomicReference<SyncedEnforcer> currentEnforcer = new AtomicReference<>();
  final ReadWriteLock reloadLock = new ReentrantReadWriteLock();
  private final JDBCAdapter adapter;
  private final String modelText;
  private final CasbinPolicyRefresher refresher;
  private final long refreshDebounceNanos;

  private final boolean refreshEnabled;

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
    this.adapter = new JDBCAdapter(driver, url, user, password);

    InputStream modelStream = this.getClass().getResourceAsStream("/jcasbin_auth_model.conf");
    this.modelText = IOUtils.toString(modelStream, StandardCharsets.UTF_8);
    currentEnforcer.set(newEnforcer());

    this.refreshDebounceNanos = serverProperties.getPolicyRefreshDebounceInterval().toNanos();
    this.refreshEnabled = serverProperties.isPolicyRefreshEnabled();
    this.refresher =
        new CasbinPolicyRefresher(this::reloadFromStore, hibernateConfigurator.getSessionFactory());
    if (refreshEnabled) {
      refresher.start(serverProperties.getPolicyRefreshInterval());
    } else {
      LOGGER.info(
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

  private SyncedEnforcer newEnforcer() {
    Model model = new Model();
    model.loadModelFromText(modelText);
    SyncedEnforcer enforcer = new SyncedEnforcer(model, adapter);
    enforcer.enableAutoSave(true);
    return enforcer;
  }

  /**
   * Rebuilds policy from {@code casbin_rule} on a new enforcer and swaps it in. Holds the exclusive
   * lock for the whole load so local grants are not applied to the outgoing instance.
   */
  void reloadFromStore() {
    reloadLock.writeLock().lock();
    try {
      currentEnforcer.set(newEnforcer());
    } finally {
      reloadLock.writeLock().unlock();
    }
  }

  /**
   * Runs a policy mutation on the live enforcer.
   *
   * <p>Takes the <em>read</em> side of {@code reloadLock} on purpose: many grants may proceed
   * together (SyncedEnforcer still serializes mutations on one instance), while reload takes the
   * write side so a mutation cannot land on an enforcer that is about to be discarded. The lock
   * names refer to the live-enforcer pointer, not to Casbin read vs write.
   */
  private <T> T mutate(Function<SyncedEnforcer, T> action) {
    reloadLock.readLock().lock();
    try {
      return action.apply(currentEnforcer.get());
    } finally {
      reloadLock.readLock().unlock();
    }
  }

  @Override
  public boolean grantAuthorization(UUID principal, UUID resource, Privileges action) {
    return mutate(e -> e.addPolicy(principal.toString(), resource.toString(), action.toString()));
  }

  @Override
  public boolean revokeAuthorization(UUID principal, UUID resource, Privileges action) {
    return mutate(
        e -> e.removePolicy(principal.toString(), resource.toString(), action.toString()));
  }

  @Override
  public boolean clearAuthorizationsForPrincipal(UUID principal) {
    return mutate(e -> e.removeFilteredPolicy(PRINCIPAL_INDEX, principal.toString()));
  }

  @Override
  public boolean clearAuthorizationsForResource(UUID resource) {
    return mutate(e -> e.removeFilteredPolicy(RESOURCE_INDEX, resource.toString()));
  }

  @Override
  public boolean addHierarchyChild(UUID parent, UUID child) {
    return mutate(
        e -> e.addNamedGroupingPolicy(HIERARCHY_POLICY, parent.toString(), child.toString()));
  }

  @Override
  public boolean removeHierarchyChild(UUID parent, UUID child) {
    return mutate(
        e -> e.removeNamedGroupingPolicy(HIERARCHY_POLICY, parent.toString(), child.toString()));
  }

  @Override
  public boolean removeHierarchyChildren(UUID resource) {
    return mutate(
        e ->
            e.removeFilteredNamedGroupingPolicy(
                HIERARCHY_POLICY, HIERARCHY_PARENT_INDEX, resource.toString()));
  }

  @Override
  public UUID getHierarchyParent(UUID resource) {
    List<List<String>> policy =
        currentEnforcer
            .get()
            .getFilteredNamedGroupingPolicy(
                HIERARCHY_POLICY, HIERARCHY_CHILD_INDEX, resource.toString());
    if (policy.isEmpty() || policy.get(0).isEmpty()) {
      return null;
    }
    return UUID.fromString(policy.get(0).get(HIERARCHY_PARENT_INDEX));
  }

  @Override
  public boolean authorize(UUID principal, UUID resource, Privileges action) {
    return currentEnforcer
        .get()
        .enforce(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean authorizeAny(UUID principal, UUID resource, Privileges... actions) {
    SyncedEnforcer enforcer = currentEnforcer.get();
    return Arrays.stream(actions)
        .anyMatch(
            action ->
                enforcer.enforce(principal.toString(), resource.toString(), action.toString()));
  }

  @Override
  public boolean authorizeAll(UUID principal, UUID resource, Privileges... actions) {
    SyncedEnforcer enforcer = currentEnforcer.get();
    return Arrays.stream(actions)
        .allMatch(
            action ->
                enforcer.enforce(principal.toString(), resource.toString(), action.toString()));
  }

  @Override
  public List<Privileges> listAuthorizations(UUID principal, UUID resource) {
    List<List<String>> list =
        currentEnforcer
            .get()
            .getPermissionsForUserInDomain(principal.toString(), resource.toString());
    return list.stream()
        .map(l -> l.get(PRIVILEGE_INDEX))
        .map(Privileges::fromValue)
        .collect(Collectors.toList());
  }

  @Override
  public Map<UUID, List<Privileges>> listAuthorizations(UUID resource) {
    return currentEnforcer.get().getFilteredPolicy(RESOURCE_INDEX, resource.toString()).stream()
        .collect(
            Collectors.groupingBy(
                l -> UUID.fromString(l.get(PRINCIPAL_INDEX)),
                Collectors.mapping(
                    l -> Privileges.fromValue(l.get(PRIVILEGE_INDEX)), Collectors.toList())));
  }

  /**
   * Rate-limited policy check before returning 403, for cross-instance create-then-read.
   *
   * @return true if the current enforcer was replaced (this call or a coalesced concurrent check)
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
    SyncedEnforcer before = currentEnforcer.get();
    refresher.checkAndReload();
    return currentEnforcer.get() != before;
  }

  @Override
  public void close() {
    refresher.close();
  }

  CasbinPolicyRefresher getRefresher() {
    return refresher;
  }
}
