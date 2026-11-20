package io.unitycatalog.server.auth;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JCasbinAuthorizerTest {
  private UnityCatalogAuthorizer authenticator;
  private HibernateConfigurator hibernateConfigurator;
  @TempDatabase private TestDatabase testDatabase;

  @BeforeEach
  void setUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(Property.SERVER_ENV.getKey(), "test");
    // Single-instance tests do not need background polling.
    properties.setProperty(Property.POLICY_REFRESH_ENABLED.getKey(), "false");
    ServerProperties serverProperties = new ServerProperties(properties);
    hibernateConfigurator = testDatabase.hibernateConfigurator();
    authenticator = new JCasbinAuthorizer(hibernateConfigurator, serverProperties);
  }

  @AfterEach
  void tearDown() {
    if (authenticator instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  void casbinCheckoutEnablesAutocommitWithoutChangingThePoolDefault() throws Exception {
    assertThat(hibernateConfigurator.getDataSource().isAutoCommit()).isFalse();
    try (Connection casbin =
            JCasbinAuthorizer.autocommitOnCheckout(hibernateConfigurator.getDataSource())
                .getConnection();
        Connection hibernate = hibernateConfigurator.getDataSource().getConnection()) {
      assertThat(casbin.getAutoCommit()).isTrue();
      assertThat(hibernate.getAutoCommit()).isFalse();
    }
  }

  @Test
  void testGrantAuthorization() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    Privileges action = Privileges.CREATE_CATALOG;

    authenticator.grantAuthorization(principal, resource, action);
    assertThat(authenticator.authorize(principal, METASTORE, resource, action)).isTrue();
  }

  @Test
  void startupIndexesExistingOwnerPolicies() throws Exception {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    authenticator.grantAuthorization(principal, resource, Privileges.OWNER);
    try (Connection connection = hibernateConfigurator.getDataSource().getConnection();
        var statement = connection.createStatement()) {
      String drop = "DROP INDEX idx_casbin_rule_resource";
      if (connection.getMetaData().getDatabaseProductName().equals("MySQL")) {
        drop += " ON casbin_rule";
      }
      statement.executeUpdate(drop);
      connection.commit();
    }
    Properties properties = new Properties();
    properties.setProperty(Property.SERVER_ENV.getKey(), "test");
    properties.setProperty(Property.POLICY_REFRESH_ENABLED.getKey(), "false");
    try (JCasbinAuthorizer restarted =
        new JCasbinAuthorizer(hibernateConfigurator, new ServerProperties(properties))) {
      assertThat(restarted.listAuthorizations(principal, resource))
          .containsExactly(Privileges.OWNER);
    }
    try (Connection connection = hibernateConfigurator.getDataSource().getConnection();
        var statement = connection.createStatement()) {
      // Dropping the index proves that startup recreated it on the existing policy table.
      statement.executeUpdate(
          "DROP INDEX idx_casbin_rule_resource"
              + (connection.getMetaData().getDatabaseProductName().equals("MySQL")
                  ? " ON casbin_rule"
                  : ""));
      connection.commit();
    }
  }

  @Test
  void loadsOnlyCommittedCreationPoliciesWithoutWritingThem() {
    UUID resource = UUID.randomUUID();
    UUID parent = UUID.randomUUID();
    UUID previousOwner = UUID.randomUUID();
    UUID currentOwner = UUID.randomUUID();
    UUID otherResource = UUID.randomUUID();
    SessionFactory sessionFactory = hibernateConfigurator.getSessionFactory();
    authenticator.grantAuthorization(previousOwner, resource, Privileges.READ_METADATA);
    sessionFactory.inTransaction(
        session -> {
          JCasbinAuthorizer.replaceOwner(session, resource, previousOwner);
          JCasbinAuthorizer.replaceOwner(session, resource, currentOwner);
          JCasbinAuthorizer.replaceOwner(session, otherResource, previousOwner);
          JCasbinAuthorizer.addHierarchyChild(session, parent, resource);
          JCasbinAuthorizer.addHierarchyChild(session, UUID.randomUUID(), otherResource);
        });
    List<Long> storedIds =
        sessionFactory.fromSession(
            session ->
                session
                    .createNativeQuery("SELECT id FROM casbin_rule ORDER BY id", Long.class)
                    .getResultList());

    assertThat(authenticator.getHierarchyParent(resource)).isNull();
    authenticator.loadOwner(resource);
    authenticator.loadOwner(UUID.randomUUID());

    assertThat(authenticator.getHierarchyParent(resource)).isEqualTo(parent);
    assertThat(authenticator.getHierarchyParent(otherResource)).isNull();
    assertThat(authenticator.addHierarchyChild(parent, resource)).isFalse();
    assertThat(authenticator.listAuthorizations(currentOwner, resource))
        .containsExactly(Privileges.OWNER);
    assertThat(authenticator.listAuthorizations(previousOwner, resource))
        .containsExactly(Privileges.READ_METADATA);
    assertThat(authenticator.listAuthorizations(previousOwner, otherResource)).isEmpty();
    List<Long> actualIds =
        sessionFactory.fromSession(
            session ->
                session
                    .createNativeQuery("SELECT id FROM casbin_rule ORDER BY id", Long.class)
                    .getResultList());
    assertThat(actualIds).containsExactlyElementsOf(storedIds);

    authenticator.grantAuthorization(currentOwner, resource, Privileges.SELECT);
    List<String> storedPrivileges =
        sessionFactory.fromSession(
            session ->
                session
                    .createNativeQuery(
                        "SELECT v2 FROM casbin_rule WHERE v0 = :principal AND v1 = :resource",
                        String.class)
                    .setParameter("principal", currentOwner.toString())
                    .setParameter("resource", resource.toString())
                    .getResultList());
    assertThat(storedPrivileges).containsExactlyInAnyOrder("OWNER", "SELECT");
  }

  @Test
  void testRevokeAuthorization() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    Privileges action = Privileges.CREATE_CATALOG;

    authenticator.grantAuthorization(principal, resource, action);
    assertThat(authenticator.authorize(principal, METASTORE, resource, action)).isTrue();
    authenticator.revokeAuthorization(principal, resource, action);
    assertThat(authenticator.authorize(principal, METASTORE, resource, action)).isFalse();
  }

  @Test
  void testClearAuthorizationsForPrincipal() {
    UUID principal = UUID.randomUUID();
    UUID principal2 = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    Privileges action = Privileges.CREATE_CATALOG;

    authenticator.grantAuthorization(principal, resource, action);
    authenticator.grantAuthorization(principal2, resource, action);
    assertThat(authenticator.authorize(principal, METASTORE, resource, action)).isTrue();
    assertThat(authenticator.authorize(principal2, METASTORE, resource, action)).isTrue();

    authenticator.clearAuthorizationsForPrincipal(principal);
    assertThat(authenticator.authorize(principal, METASTORE, resource, action)).isFalse();
    assertThat(authenticator.authorize(principal2, METASTORE, resource, action)).isTrue();

    authenticator.clearAuthorizationsForPrincipal(principal2);
    assertThat(authenticator.authorize(principal2, METASTORE, resource, action)).isFalse();
  }

  @Test
  void testAddHierarchyChild() {
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    Privileges action = Privileges.SELECT;

    authenticator.addHierarchyChild(catalog, schema);
    authenticator.grantAuthorization(principal, catalog, action);
    assertThat(authenticator.authorize(principal, SCHEMA, schema, action)).isTrue();
  }

  @Test
  void testRemoveHierarchyChild() {
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    Privileges action = Privileges.SELECT;

    authenticator.addHierarchyChild(catalog, schema);
    authenticator.grantAuthorization(principal, catalog, action);
    assertThat(authenticator.authorize(principal, SCHEMA, schema, action)).isTrue();
    authenticator.removeHierarchyChild(catalog, schema);
    assertThat(authenticator.authorize(principal, SCHEMA, schema, action)).isFalse();
  }

  @Test
  void testRemoveHierarchyChildren() {
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    Privileges action = Privileges.SELECT;

    authenticator.addHierarchyChild(catalog, schema);
    authenticator.grantAuthorization(principal, catalog, action);
    assertThat(authenticator.authorize(principal, SCHEMA, schema, action)).isTrue();
    authenticator.removeHierarchyChildren(catalog);
    assertThat(authenticator.authorize(principal, SCHEMA, schema, action)).isFalse();
  }

  @Test
  void testAuthorizeAny() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();

    assertThat(
            authenticator.authorizeAny(
                principal, METASTORE, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG))
        .isFalse();
    authenticator.grantAuthorization(principal, resource, Privileges.USE_CATALOG);
    assertThat(
            authenticator.authorizeAny(
                principal, METASTORE, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG))
        .isTrue();
  }

  @Test
  void testAuthorizeAll() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();

    assertThat(
            authenticator.authorizeAll(
                principal, METASTORE, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG))
        .isFalse();
    authenticator.grantAuthorization(principal, resource, Privileges.USE_CATALOG);
    assertThat(
            authenticator.authorizeAll(
                principal, METASTORE, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG))
        .isFalse();
    authenticator.grantAuthorization(principal, resource, Privileges.CREATE_CATALOG);
    assertThat(
            authenticator.authorizeAll(
                principal, METASTORE, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG))
        .isTrue();
  }

  @Test
  void testListAuthorizations() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    List<Privileges> actions = Arrays.asList(Privileges.USE_CATALOG, Privileges.CREATE_CATALOG);

    assertThat(authenticator.listAuthorizations(principal, resource)).isEmpty();
    actions.forEach(action -> authenticator.grantAuthorization(principal, resource, action));
    List<Privileges> result = authenticator.listAuthorizations(principal, resource);
    assertThat(result).isEqualTo(actions);
  }

  @Test
  void testListAuthorizationsForAllUsers() {
    UUID principal = UUID.randomUUID();
    UUID principal2 = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    UUID resource2 = UUID.randomUUID();

    List<Privileges> actions = Arrays.asList(Privileges.USE_CATALOG, Privileges.CREATE_CATALOG);
    List<Privileges> actions2 = Arrays.asList(Privileges.CREATE_CATALOG, Privileges.SELECT);
    Map<UUID, List<Privileges>> empty = authenticator.listAuthorizations(resource);
    assertThat(empty).isEmpty();

    actions.forEach(action -> authenticator.grantAuthorization(principal, resource, action));
    actions.forEach(action -> authenticator.grantAuthorization(principal, resource2, action));
    actions2.forEach(action -> authenticator.grantAuthorization(principal2, resource, action));
    Map<UUID, List<Privileges>> expected = Map.of(principal, actions, principal2, actions2);
    assertThat(authenticator.listAuthorizations(resource)).isEqualTo(expected);
  }

  @Test
  void failedDeleteThrowsTheDatabaseError() throws Exception {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    authenticator.grantAuthorization(principal, resource, Privileges.OWNER);
    try (Connection connection = hibernateConfigurator.getDataSource().getConnection()) {
      connection.createStatement().execute("DROP TABLE casbin_rule");
      connection.commit();
    }
    // jdbc-adapter retries the delete, then throws an Error that wraps the SQLException.
    assertThatThrownBy(
            () -> authenticator.revokeAuthorization(principal, resource, Privileges.OWNER))
        .isInstanceOf(Error.class)
        .hasCauseInstanceOf(SQLException.class);
  }
}
