package io.unitycatalog.server.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.unitycatalog.server.persist.model.Privileges;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JCasbinAuthorizerTest {
  private UnityCatalogAuthorizer authenticator;

  @BeforeEach
  void setUp() throws Exception {
    System.setProperty("server.env", "test");
    authenticator = new JCasbinAuthorizer();
  }

  @Test
  void testGrantAuthorization() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    Privileges action = Privileges.CREATE_CATALOG;

    authenticator.grantAuthorization(principal, resource, action);
    assertTrue(authenticator.authorize(principal, resource, action));
  }

  @Test
  void testRevokeAuthorization() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    Privileges action = Privileges.CREATE_CATALOG;

    authenticator.grantAuthorization(principal, resource, action);
    assertTrue(authenticator.authorize(principal, resource, action));
    authenticator.revokeAuthorization(principal, resource, action);
    assertFalse(authenticator.authorize(principal, resource, action));
  }

  @Test
  void testClearAuthorizationsForPrincipal() {
    UUID principal = UUID.randomUUID();
    UUID principal2 = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    Privileges action = Privileges.CREATE_CATALOG;

    authenticator.grantAuthorization(principal, resource, action);
    authenticator.grantAuthorization(principal2, resource, action);
    assertTrue(authenticator.authorize(principal, resource, action));
    assertTrue(authenticator.authorize(principal2, resource, action));

    authenticator.clearAuthorizationsForPrincipal(principal);
    assertFalse(authenticator.authorize(principal, resource, action));
    assertTrue(authenticator.authorize(principal2, resource, action));

    authenticator.clearAuthorizationsForPrincipal(principal2);
    assertFalse(authenticator.authorize(principal2, resource, action));
  }

  @Test
  void testAddHierarchyChild() {
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    Privileges action = Privileges.SELECT;

    authenticator.addHierarchyChild(catalog, schema);
    authenticator.grantAuthorization(principal, catalog, action);
    assertTrue(authenticator.authorize(principal, schema, action));
  }

  @Test
  void testRemoveHierarchyChild() {
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    Privileges action = Privileges.SELECT;

    authenticator.addHierarchyChild(catalog, schema);
    authenticator.grantAuthorization(principal, catalog, action);
    assertTrue(authenticator.authorize(principal, schema, action));
    authenticator.removeHierarchyChild(catalog, schema);
    assertFalse(authenticator.authorize(principal, schema, action));
  }

  @Test
  void testRemoveHierarchyChildren() {
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    Privileges action = Privileges.SELECT;

    authenticator.addHierarchyChild(catalog, schema);
    authenticator.grantAuthorization(principal, catalog, action);
    assertTrue(authenticator.authorize(principal, schema, action));
    authenticator.removeHierarchyChildren(catalog);
    assertFalse(authenticator.authorize(principal, schema, action));
  }

  @Test
  void testAuthorizeAny() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();

    assertFalse(
        authenticator.authorizeAny(
            principal, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG));
    authenticator.grantAuthorization(principal, resource, Privileges.USE_CATALOG);
    assertTrue(
        authenticator.authorizeAny(
            principal, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG));
  }

  @Test
  void testAuthorizeAll() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();

    assertFalse(
        authenticator.authorizeAll(
            principal, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG));
    authenticator.grantAuthorization(principal, resource, Privileges.USE_CATALOG);
    assertFalse(
        authenticator.authorizeAll(
            principal, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG));
    authenticator.grantAuthorization(principal, resource, Privileges.CREATE_CATALOG);
    assertTrue(
        authenticator.authorizeAll(
            principal, resource, Privileges.USE_CATALOG, Privileges.CREATE_CATALOG));
  }

  @Test
  void testListAuthorizations() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    List<Privileges> actions = Arrays.asList(Privileges.USE_CATALOG, Privileges.CREATE_CATALOG);

    assertTrue(authenticator.listAuthorizations(principal, resource).isEmpty());
    actions.forEach(action -> authenticator.grantAuthorization(principal, resource, action));
    List<Privileges> result = authenticator.listAuthorizations(principal, resource);
    assertEquals(actions, result);
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
    assertTrue(empty.isEmpty());

    actions.forEach(action -> authenticator.grantAuthorization(principal, resource, action));
    actions.forEach(action -> authenticator.grantAuthorization(principal, resource2, action));
    actions2.forEach(action -> authenticator.grantAuthorization(principal2, resource, action));
    Map<UUID, List<Privileges>> expected = Map.of(principal, actions, principal2, actions2);
    assertEquals(expected, authenticator.listAuthorizations(resource));
  }
}
