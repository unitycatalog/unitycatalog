package io.unitycatalog.server.auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.User;
import io.unitycatalog.server.persist.UserRepository;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class JCasbinAuthorizerTest {
  private static UserRepository mockUserRepository = Mockito.mock(UserRepository.class);
  private UnityCatalogAuthorizer authenticator;

  @BeforeEach
  void setUp() throws Exception {
    System.setProperty("server.env", "test");
    authenticator =
        new JCasbinAuthorizer() {
          {
            USER_REPOSITORY = mockUserRepository;
          }
        };
  }

  @Test
  void testGrantAuthorization() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    Privilege action = Privilege.CREATE_CATALOG;

    authenticator.grantAuthorization(principal, resource, action);
    assertTrue(authenticator.authorize(principal, resource, action));
  }

  @Test
  void testRevokeAuthorization() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    Privilege action = Privilege.CREATE_CATALOG;

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
    Privilege action = Privilege.CREATE_CATALOG;

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
    Privilege action = Privilege.SELECT;

    authenticator.addHierarchyChild(catalog, schema);
    authenticator.grantAuthorization(principal, catalog, action);
    assertTrue(authenticator.authorize(principal, schema, action));
  }

  @Test
  void testRemoveHierarchyChild() {
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    Privilege action = Privilege.SELECT;

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
    Privilege action = Privilege.SELECT;

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
            principal, resource, Privilege.USE_CATALOG, Privilege.CREATE_CATALOG));
    authenticator.grantAuthorization(principal, resource, Privilege.USE_CATALOG);
    assertTrue(
        authenticator.authorizeAny(
            principal, resource, Privilege.USE_CATALOG, Privilege.CREATE_CATALOG));
  }

  @Test
  void testAuthorizeAll() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();

    assertFalse(
        authenticator.authorizeAll(
            principal, resource, Privilege.USE_CATALOG, Privilege.CREATE_CATALOG));
    authenticator.grantAuthorization(principal, resource, Privilege.USE_CATALOG);
    assertFalse(
        authenticator.authorizeAll(
            principal, resource, Privilege.USE_CATALOG, Privilege.CREATE_CATALOG));
    authenticator.grantAuthorization(principal, resource, Privilege.CREATE_CATALOG);
    assertTrue(
        authenticator.authorizeAll(
            principal, resource, Privilege.USE_CATALOG, Privilege.CREATE_CATALOG));
  }

  @Test
  void testListAuthorizations() {
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    List<Privilege> actions = Arrays.asList(Privilege.USE_CATALOG, Privilege.CREATE_CATALOG);

    assertTrue(authenticator.listAuthorizations(principal, resource).isEmpty());
    actions.forEach(action -> authenticator.grantAuthorization(principal, resource, action));
    List<Privilege> result = authenticator.listAuthorizations(principal, resource);
    assertEquals(actions, result);
  }

  @Test
  void testListAuthorizationsForAllUsers() {
    UUID principal = UUID.randomUUID();
    UUID principal2 = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    UUID resource2 = UUID.randomUUID();
    when(mockUserRepository.listUsers())
        .thenReturn(
            Stream.of(principal, principal2)
                .map(UUID::toString)
                .map(u -> new User().id(u))
                .toList());

    List<Privilege> actions = Arrays.asList(Privilege.USE_CATALOG, Privilege.CREATE_CATALOG);
    List<Privilege> actions2 = Arrays.asList(Privilege.CREATE_CATALOG, Privilege.SELECT);
    Map<UUID, List<Privilege>> empty = authenticator.listAuthorizations(resource);
    assertTrue(empty.isEmpty());

    actions.forEach(action -> authenticator.grantAuthorization(principal, resource, action));
    actions.forEach(action -> authenticator.grantAuthorization(principal, resource2, action));
    actions2.forEach(action -> authenticator.grantAuthorization(principal2, resource, action));
    Map<UUID, List<Privilege>> expected = Map.of(principal, actions, principal2, actions2);
    assertEquals(expected, authenticator.listAuthorizations(resource));
  }
}
