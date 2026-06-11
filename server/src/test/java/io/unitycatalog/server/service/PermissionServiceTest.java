package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_NAME;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.api.TablesApi;
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
import org.junit.jupiter.api.BeforeEach;
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

  @BeforeEach
  @SneakyThrows
  public void setUp() {
    super.setUp();
    createTestUser(REGULAR_1);
    createTestUser(REGULAR_2);
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  @Test
  @SneakyThrows
  public void testPermissionsServiceUseCases() {
    // Grants are scoped to a single securable, so we exercise the catalog, schema and table levels
    // independently and then read each one back. The base test only creates the catalog and schema,
    // so the table is created here before privileges can be granted on it.
    createExternalTable(
        new TablesApi(adminApiClient), CATALOG_NAME, SCHEMA_NAME, TABLE_NAME, "/tmp/" + TABLE_NAME);

    // Admin (metastore owner) grants catalog-level privileges.
    grantsApi.update(
        SecurableType.CATALOG,
        CATALOG_NAME,
        addPrivileges(REGULAR_1, Privilege.USE_CATALOG, Privilege.CREATE_SCHEMA));
    grantsApi.update(
        SecurableType.CATALOG, CATALOG_NAME, addPrivileges(REGULAR_2, Privilege.USE_CATALOG));

    // Schema-level privileges for regular-1.
    grantsApi.update(
        SecurableType.SCHEMA,
        SCHEMA_FULL_NAME,
        addPrivileges(REGULAR_1, Privilege.USE_SCHEMA, Privilege.CREATE_TABLE));

    // Table-level privileges for regular-1.
    grantsApi.update(
        SecurableType.TABLE,
        TABLE_FULL_NAME,
        addPrivileges(REGULAR_1, Privilege.SELECT, Privilege.MODIFY));

    // Reading back as the owner exposes the full ACL. Each securable is queried separately because
    // a GET only returns the privileges granted directly on that securable.
    PermissionsList catalogPermissions = grantsApi.get(SecurableType.CATALOG, CATALOG_NAME, null);
    assertThat(privilegesFor(catalogPermissions, REGULAR_1))
        .containsExactlyInAnyOrder(Privilege.USE_CATALOG, Privilege.CREATE_SCHEMA);
    assertThat(privilegesFor(catalogPermissions, REGULAR_2)).containsExactly(Privilege.USE_CATALOG);

    PermissionsList schemaPermissions = grantsApi.get(SecurableType.SCHEMA, SCHEMA_FULL_NAME, null);
    assertThat(privilegesFor(schemaPermissions, REGULAR_1))
        .containsExactlyInAnyOrder(Privilege.USE_SCHEMA, Privilege.CREATE_TABLE);
    assertThat(privilegesFor(schemaPermissions, REGULAR_2)).isEmpty();

    PermissionsList tablePermissions = grantsApi.get(SecurableType.TABLE, TABLE_FULL_NAME, null);
    assertThat(privilegesFor(tablePermissions, REGULAR_1))
        .containsExactlyInAnyOrder(Privilege.SELECT, Privilege.MODIFY);
    assertThat(privilegesFor(tablePermissions, REGULAR_2)).isEmpty();
  }

  @Test
  @SneakyThrows
  public void nonOwnerCannotGrantPermissionsToOthers() {
    // regular-1 cannot grant permissions to regular-2
    assertPermissionDenied(
        () ->
            grantsApiFor(REGULAR_1)
                .update(
                    SecurableType.CATALOG,
                    CATALOG_NAME,
                    addPrivileges(REGULAR_2, Privilege.USE_CATALOG)));
  }

  @Test
  @SneakyThrows
  public void nonOwnerCanReadCatalogPermissions() {
    // GET is gated only on authentication, so a non-owner gets their own (empty) view.
    PermissionsList response =
        grantsApiFor(REGULAR_1).get(SecurableType.CATALOG, CATALOG_NAME, null);
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

  private static UpdatePermissions addPrivileges(String principal, Privilege... privileges) {
    PermissionsChange change =
        new PermissionsChange().principal(principal).add(List.of(privileges)).remove(List.of());
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
