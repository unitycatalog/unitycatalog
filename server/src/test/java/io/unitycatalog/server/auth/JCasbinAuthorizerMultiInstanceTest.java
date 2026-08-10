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
 * Cross-replica Casbin policy propagation. Each {@link JCasbinAuthorizer} is a separate in-memory
 * enforcer against one shared database; assertions poll because propagation is asynchronous.
 */
public class JCasbinAuthorizerMultiInstanceTest {

  private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(5);
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

  private JCasbinAuthorizer startReplica() throws Exception {
    return register(
        new JCasbinAuthorizer(hibernateConfigurator, properties(true, TEST_REFRESH_INTERVAL)));
  }

  private JCasbinAuthorizer startReplicaWithoutRefresh() throws Exception {
    return register(
        new JCasbinAuthorizer(hibernateConfigurator, properties(false, TEST_REFRESH_INTERVAL)));
  }

  private JCasbinAuthorizer register(JCasbinAuthorizer authorizer) {
    replicas.add(authorizer);
    return authorizer;
  }

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

  @Test
  void detectsAnInsert() throws Exception {
    UnityCatalogAuthorizer writer = startReplicaWithoutRefresh();
    JCasbinAuthorizer reader = startReplicaWithoutRefresh();
    settle(reader);

    writer.grantAuthorization(UUID.randomUUID(), UUID.randomUUID(), Privileges.SELECT);

    assertThat(reader.getRefresher().checkAndReload()).isTrue();
  }

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

  @Test
  void denyTriggeredRefreshIsDebounced() throws Exception {
    JCasbinAuthorizer replica = startReplica();

    assertThat(replica.refreshAuthorizations()).isTrue();
    assertThat(replica.refreshAuthorizations()).isFalse();
  }

  @Test
  void disablingRefreshAlsoDisablesTheDenyTriggeredReload() throws Exception {
    JCasbinAuthorizer replica = startReplicaWithoutRefresh();

    assertThat(replica.getRefresher().isRunning()).isFalse();
    assertThat(replica.refreshAuthorizations()).isFalse();
  }

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

  @Test
  void closeStopsTheRefreshThread() throws Exception {
    JCasbinAuthorizer replica = startReplica();
    assertThat(replica.getRefresher().isRunning()).isTrue();

    replica.close();

    assertThat(replica.getRefresher().isRunning()).isFalse();
  }
}
