package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.PermissionsList;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.PrivilegeAssignment;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.access.SdkAccessControlBaseCRUDTest;
import io.unitycatalog.server.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link PermissionService} with authorization enabled.
 *
 * <p>A real Unity Catalog server is booted (via {@link SdkAccessControlBaseCRUDTest}) with {@code
 * server.authorization=enable} and the permission endpoints are exercised end-to-end through the
 * generated {@link GrantsApi} SDK client (built with {@link TestUtils#createApiClient}). Driving
 * the SDK rather than a raw HTTP client also exercises the Armeria authentication ({@code
 * AuthDecorator}) and authorization ({@code UnityAccessDecorator}) decorators wrapping the service.
 */
public class PermissionServiceTest extends SdkAccessControlBaseCRUDTest {

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  @Test
  @SneakyThrows
  public void ownerCanGrantAndReadCatalogPermissions() {
    createTestUser(REGULAR_1);

    // Admin (metastore owner) grants USE CATALOG to regular-1.
    PermissionsList grantResponse =
        grantsApi.update(
            SecurableType.CATALOG, CATALOG_NAME, addPrivilege(REGULAR_1, Privilege.USE_CATALOG));
    assertThat(privilegesFor(grantResponse, REGULAR_1)).containsExactly(Privilege.USE_CATALOG);

    // Reading back as the owner exposes the full ACL, including regular-1's grant.
    PermissionsList getResponse = grantsApi.get(SecurableType.CATALOG, CATALOG_NAME, null);
    assertThat(privilegesFor(getResponse, REGULAR_1)).containsExactly(Privilege.USE_CATALOG);
  }

  @Test
  @SneakyThrows
  public void nonOwnerCannotUpdateCatalogPermissions() {
    createTestUser(REGULAR_1);

    // regular-1 is neither metastore nor catalog owner, so the decorator rejects the update.
    GrantsApi regularGrantsApi = grantsApiFor(REGULAR_1);
    assertPermissionDenied(
        () ->
            regularGrantsApi.update(
                SecurableType.CATALOG,
                CATALOG_NAME,
                addPrivilege(REGULAR_1, Privilege.USE_CATALOG)));
  }

  @Test
  @SneakyThrows
  public void nonOwnerCanReadCatalogPermissions() {
    createTestUser(REGULAR_1);

    // GET is gated only on authentication, so a non-owner gets their own (empty) view.
    GrantsApi regularGrantsApi = grantsApiFor(REGULAR_1);
    PermissionsList response = regularGrantsApi.get(SecurableType.CATALOG, CATALOG_NAME, null);
    assertThat(privilegesFor(response, REGULAR_1)).isEmpty();
  }

  @Test
  public void unauthenticatedRequestIsRejected() {
    // No bearer token -> the AuthDecorator rejects the request before it reaches the service.
    GrantsApi unauthGrantsApi = new GrantsApi(TestUtils.createApiClient(serverConfig));
    assertApiException(
        () -> unauthGrantsApi.get(SecurableType.CATALOG, CATALOG_NAME, null),
        ErrorCode.UNAUTHENTICATED,
        "authorization");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Builds a {@link GrantsApi} client authenticated as the given test user. */
  private GrantsApi grantsApiFor(String userEmail) {
    ServerConfig userConfig = createTestUserServerConfig(userEmail);
    return new GrantsApi(TestUtils.createApiClient(userConfig));
  }

  private static UpdatePermissions addPrivilege(String principal, Privilege privilege) {
    PermissionsChange change =
        new PermissionsChange().principal(principal).add(List.of(privilege)).remove(List.of());
    return new UpdatePermissions().changes(List.of(change));
  }

  /** Returns the privileges assigned to {@code principal} in a PermissionsList response. */
  private static List<Privilege> privilegesFor(PermissionsList permissionsList, String principal) {
    List<Privilege> privileges = new ArrayList<>();
    if (permissionsList.getPrivilegeAssignments() == null) {
      return privileges;
    }
    for (PrivilegeAssignment assignment : permissionsList.getPrivilegeAssignments()) {
      if (principal.equals(assignment.getPrincipal())) {
        privileges.addAll(assignment.getPrivileges());
      }
    }
    return privileges;
  }
}
