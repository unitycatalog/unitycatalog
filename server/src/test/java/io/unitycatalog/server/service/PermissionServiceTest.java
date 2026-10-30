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
import io.unitycatalog.server.auth.JCasbinAuthorizer;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.sdk.access.SdkAccessControlBaseCRUDTest;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    observabilityEnabled = true;
  }

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

    // A securable that does not exist is a not-found, whatever privileges the caller holds. The
    // volume case is the one worth reading back: the securable is resolved through
    // VolumeRepository.getVolume, which used its lookup without checking it found anything, so this
    // endpoint answered a 500 that named an internal class.
    String missingVolume = SCHEMA_FULL_NAME + ".no_such_volume";
    assertApiException(
        () -> grantsApi.get(SecurableType.VOLUME, missingVolume, null),
        ErrorCode.NOT_FOUND,
        "Volume not found: " + missingVolume);
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
    assertHttpRequestMetric("updateCatalogAuthorization", 403);
  }

  @Test
  @SneakyThrows
  public void invalidAdditionLeavesAllGrantsUnchanged() {
    grantsApi.update(
        SecurableType.SCHEMA, SCHEMA_FULL_NAME, addPrivileges(REGULAR_1, Privilege.USE_SCHEMA));

    UpdatePermissions request =
        new UpdatePermissions()
            .changes(
                List.of(
                    new PermissionsChange()
                        .principal(REGULAR_1)
                        .add(List.of(Privilege.CREATE_TABLE))
                        .remove(List.of(Privilege.USE_SCHEMA)),
                    new PermissionsChange()
                        .principal(REGULAR_2)
                        .add(List.of(Privilege.USE_SCHEMA, Privilege.USE_CATALOG))
                        .remove(List.of())));
    assertApiException(
        () -> grantsApi.update(SecurableType.SCHEMA, SCHEMA_FULL_NAME, request),
        ErrorCode.INVALID_ARGUMENT,
        "Privilege USE CATALOG cannot be granted on schema");

    PermissionsList permissions = grantsApi.get(SecurableType.SCHEMA, SCHEMA_FULL_NAME, null);
    assertThat(privilegesFor(permissions, REGULAR_1)).containsExactly(Privilege.USE_SCHEMA);
    assertThat(privilegesFor(permissions, REGULAR_2)).isEmpty();
  }

  @Test
  @SneakyThrows
  public void legacyInvalidGrantCanBeRemoved() {
    ServerProperties properties = new ServerProperties(serverProperties);
    Repositories repositories =
        new Repositories(hibernateConfigurator.getSessionFactory(), properties);
    UUID principalId =
        UUID.fromString(repositories.getUserRepository().getUserByEmail(REGULAR_1).getId());
    UUID schemaId =
        UUID.fromString(
            repositories.getSchemaRepository().getSchema(SCHEMA_FULL_NAME).getSchemaId());

    try (JCasbinAuthorizer authorizer = new JCasbinAuthorizer(hibernateConfigurator, properties)) {
      // Seed an old invalid grant directly because the API must no longer create it.
      authorizer.grantAuthorization(principalId, schemaId, Privileges.USE_CATALOG);
      assertThat(countCasbinRulesReferencing(principalId.toString())).isOne();

      new PermissionService(authorizer, repositories, properties)
          .updateSchemaAuthorization(
              SCHEMA_FULL_NAME,
              new io.unitycatalog.server.model.UpdatePermissions()
                  .changes(
                      List.of(
                          new io.unitycatalog.server.model.PermissionsChange()
                              .principal(REGULAR_1)
                              .remove(
                                  List.of(io.unitycatalog.server.model.Privilege.USE_CATALOG)))));

      assertThat(authorizer.listAuthorizations(principalId, schemaId)).isEmpty();
      assertThat(countCasbinRulesReferencing(principalId.toString())).isZero();
    }
  }

  @Test
  @SneakyThrows
  public void validParentGrantsStillApplyToChildObjects() {
    TablesApi adminTables = new TablesApi(adminApiClient);
    TablesApi readerTables =
        new TablesApi(TestUtils.createApiClient(createTestUserServerConfig(REGULAR_1)));
    createExternalTable(
        adminTables,
        CATALOG_NAME,
        SCHEMA_NAME,
        TABLE_NAME,
        testDirectoryRoot.resolve(TABLE_NAME).toString());
    grantsApi.update(
        SecurableType.CATALOG,
        CATALOG_NAME,
        addPrivileges(REGULAR_1, Privilege.USE_CATALOG, Privilege.USE_SCHEMA, Privilege.SELECT));
    assertThat(getTable(readerTables, TABLE_FULL_NAME).getName()).isEqualTo(TABLE_NAME);

    String futureTable = TABLE_NAME + "_future";
    String futureFullName = SCHEMA_FULL_NAME + "." + futureTable;
    createExternalTable(
        adminTables,
        CATALOG_NAME,
        SCHEMA_NAME,
        futureTable,
        testDirectoryRoot.resolve(futureTable).toString());
    assertThat(getTable(readerTables, futureFullName).getName()).isEqualTo(futureTable);

    grantsApi.update(
        SecurableType.TABLE, TABLE_FULL_NAME, addPrivileges(REGULAR_1, Privilege.SELECT));
    grantsApi.update(
        SecurableType.CATALOG, CATALOG_NAME, removePrivileges(REGULAR_1, Privilege.SELECT));
    assertThat(getTable(readerTables, TABLE_FULL_NAME).getName()).isEqualTo(TABLE_NAME);
    assertPermissionDenied(() -> getTable(readerTables, futureFullName));

    grantsApi.update(
        SecurableType.SCHEMA, SCHEMA_FULL_NAME, addPrivileges(REGULAR_1, Privilege.SELECT));
    grantsApi.update(
        SecurableType.TABLE, TABLE_FULL_NAME, removePrivileges(REGULAR_1, Privilege.SELECT));
    assertThat(getTable(readerTables, TABLE_FULL_NAME).getName()).isEqualTo(TABLE_NAME);
    assertThat(getTable(readerTables, futureFullName).getName()).isEqualTo(futureTable);

    grantsApi.update(
        SecurableType.SCHEMA, SCHEMA_FULL_NAME, addPrivileges(REGULAR_1, Privilege.CREATE_TABLE));
    String createdTable = TABLE_NAME + "_created";
    assertThat(
            createExternalTable(
                    readerTables,
                    CATALOG_NAME,
                    SCHEMA_NAME,
                    createdTable,
                    testDirectoryRoot.resolve(createdTable).toString())
                .getName())
        .isEqualTo(createdTable);
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
    assertHttpRequestMetric("getCatalogAuthorization", 401);
  }

  @Test
  public void tokenForAMissingUserIsDenied() {
    // A valid internal token for a user that was never created -> the AuthDecorator's user lookup
    // finds nothing and denies the request, rather than letting it reach the service.
    GrantsApi missingUserGrantsApi =
        new GrantsApi(
            TestUtils.createApiClient(createTestUserServerConfig("missing-user@example.com")));
    assertApiException(
        () -> missingUserGrantsApi.get(SecurableType.CATALOG, CATALOG_NAME, null),
        ErrorCode.PERMISSION_DENIED,
        "User not allowed: missing-user@example.com");
    assertHttpRequestMetric("getCatalogAuthorization", 403);
  }

  @Test
  @SneakyThrows
  public void unknownProtectedEndpointIsRecordedWithAndWithoutAuthentication() {
    String path = "/api/2.1/unity-catalog/does-not-exist";
    String fallbackService = "com.linecorp.armeria.server.FallbackService";

    var unauthenticatedResponse = TestUtils.sendRawGet(serverConfig, path);
    assertThat(unauthenticatedResponse.statusCode()).isEqualTo(401);
    TestUtils.assertHttpRequestMetric(
        serverConfig, observabilityServerConfig, fallbackService, "GET", 401, 1.0);

    var authenticatedResponse = TestUtils.sendRawGet(adminConfig, path);
    // Authorization currently turns an unmatched authenticated route into 500. Allow 404 so a
    // future routing fix does not change this test's metric-counting contract.
    assertThat(authenticatedResponse.statusCode()).isIn(404, 500);
    TestUtils.assertHttpRequestMetric(
        serverConfig,
        observabilityServerConfig,
        fallbackService,
        "GET",
        authenticatedResponse.statusCode(),
        1.0);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Builds a {@link GrantsApi} client authenticated as the given test user. */
  private GrantsApi grantsApiFor(String userEmail) {
    ServerConfig userConfig = createTestUserServerConfig(userEmail);
    return new GrantsApi(TestUtils.createApiClient(userConfig));
  }

  @SneakyThrows
  private void assertHttpRequestMetric(String method, int status) {
    TestUtils.assertHttpRequestMetric(
        serverConfig,
        observabilityServerConfig,
        "io.unitycatalog.server.service.PermissionService",
        method,
        status,
        1.0);
  }

  private static UpdatePermissions addPrivileges(String principal, Privilege... privileges) {
    PermissionsChange change =
        new PermissionsChange().principal(principal).add(List.of(privileges)).remove(List.of());
    return new UpdatePermissions().changes(List.of(change));
  }

  private static UpdatePermissions removePrivileges(String principal, Privilege... privileges) {
    PermissionsChange change =
        new PermissionsChange().principal(principal).add(List.of()).remove(List.of(privileges));
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
