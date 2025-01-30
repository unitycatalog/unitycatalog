package io.unitycatalog.server.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JCasbinAuthorizerTest {
  private UnityCatalogAuthorizer authenticator;

  @BeforeEach
  void setUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    ServerProperties serverProperties = new ServerProperties(properties);
    HibernateConfigurator hibernateConfigurator = new HibernateConfigurator(serverProperties);
    authenticator = new JCasbinAuthorizer(hibernateConfigurator);
  }

  @Test
  void testGrantAuthorization() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    Privileges action = Privileges.CREATE_CATALOG;

    authenticator.grantAuthorization(principal, resource, action);
    assertThat(authenticator.authorize(principal, resource, action)).isTrue();
  }

  @Test
  void testRevokeAuthorization() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    Privileges action = Privileges.CREATE_CATALOG;

    authenticator.grantAuthorization(principal, resource, action);
    assertThat(authenticator.authorize(principal, resource, action)).isTrue();
    authenticator.revokeAuthorization(principal, resource, action);
    assertThat(authenticator.authorize(principal, resource, action)).isFalse();
  }

  @Test
  void testClearAuthorizationsForPrincipal() {
    UUID principal = UUID.randomUUID();
    UUID principal2 = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    Privileges action = Privileges.CREATE_CATALOG;

    authenticator.grantAuthorization(principal, resource, action);
    authenticator.grantAuthorization(principal2, resource, action);
    assertThat(authenticator.authorize(principal, resource, action)).isTrue();
    assertThat(authenticator.authorize(principal2, resource, action)).isTrue();

    authenticator.clearAuthorizationsForPrincipal(principal);
    assertThat(authenticator.authorize(principal, resource, action)).isFalse();
    assertThat(authenticator.authorize(principal2, resource, action)).isTrue();

    authenticator.clearAuthorizationsForPrincipal(principal2);
    assertThat(authenticator.authorize(principal2, resource, action)).isFalse();
  }

  @Test
  void testAddHierarchyChild() {
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    Privileges action = Privileges.SELECT;

    authenticator.addHierarchyChild(catalog, schema);
    authenticator.grantAuthorization(principal, catalog, action);
    assertThat(authenticator.authorize(principal, schema, action)).isTrue();
  }

  @Test
  void testRemoveHierarchyChild() {
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    Privileges action = Privileges.SELECT;

    authenticator.addHierarchyChild(catalog, schema);
    authenticator.grantAuthorization(principal, catalog, action);
    assertThat(authenticator.authorize(principal, schema, action)).isTrue();
    authenticator.removeHierarchyChild(catalog, schema);
    assertThat(authenticator.authorize(principal, schema, action)).isFalse();
  }

  @Test
  void testRemoveHierarchyChildren() {
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    Privileges action = Privileges.SELECT;

    authenticator.addHierarchyChild(catalog, schema);
    authenticator.grantAuthorization(principal, catalog, action);
    assertThat(authenticator.authorize(principal, schema, action)).isTrue();
    authenticator.removeHierarchyChildren(catalog);
    assertThat(authenticator.authorize(principal, schema, action)).isFalse();
  }

  @Test
  void testAuthorizeAny() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();

    assertThat(
            authenticator.authorizeAny(
                principal, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG))
        .isFalse();
    authenticator.grantAuthorization(principal, resource, Privileges.USE_CATALOG);
    assertThat(
            authenticator.authorizeAny(
                principal, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG))
        .isTrue();
  }

  @Test
  void testAuthorizeAll() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();

    assertThat(
            authenticator.authorizeAll(
                principal, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG))
        .isFalse();
    authenticator.grantAuthorization(principal, resource, Privileges.USE_CATALOG);
    assertThat(
            authenticator.authorizeAll(
                principal, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG))
        .isFalse();
    authenticator.grantAuthorization(principal, resource, Privileges.CREATE_CATALOG);
    assertThat(
            authenticator.authorizeAll(
                principal, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG))
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
}
