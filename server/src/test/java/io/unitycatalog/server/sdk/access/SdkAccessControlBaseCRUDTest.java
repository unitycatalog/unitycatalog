package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.control.api.UsersApi;
import io.unitycatalog.control.model.Email;
import io.unitycatalog.control.model.UserResource;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.access.BaseAccessControlCRUDTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for SDK-based access control CRUD tests that provides common support functionality.
 *
 * <p>This class extends {@link BaseAccessControlCRUDTest} and provides additional utilities for
 * SDK-based tests, including:
 *
 * <ul>
 *   <li>Schema and table operations setup
 *   <li>User management (creating test users)
 *   <li>Permission management (granting/revoking permissions)
 *   <li>JWT token generation for test users
 *   <li>Common resource creation (catalogs, schemas)
 *   <li>Automatic cleanup of created users and granted permissions
 * </ul>
 */
public abstract class SdkAccessControlBaseCRUDTest extends BaseAccessControlCRUDTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SdkAccessControlBaseCRUDTest.class);

  public static final String METASTORE_NAME = "metastore"; // The name of metastore is this

  protected ServerConfig adminConfig;
  protected UsersApi usersApi;
  protected GrantsApi grantsApi;

  // Track created users and granted permissions for cleanup
  private final List<String> createdUserIds = new ArrayList<>();
  private final List<PermissionGrant> grantedPermissions = new ArrayList<>();

  /** Helper class to track granted permissions for cleanup. */
  private record PermissionGrant(
      String userEmail,
      SecurableType resourceSecurableType,
      String resourceFullName,
      List<Privilege> privileges) {}

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  /**
   * Creates schema operations for the given server configuration.
   *
   * @param config Server configuration including URL and auth token
   * @return A SchemaOperations instance
   */
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    // Use admin token for creating shared resources
    String adminToken = securityContext.createServiceToken();
    adminConfig = new ServerConfig(serverConfig.getServerUrl(), adminToken);
    usersApi = new UsersApi(createControlApiClient(adminConfig));
    grantsApi = new GrantsApi(TestUtils.createApiClient(adminConfig));
    createCommonResources();
  }

  @AfterEach
  public void tearDown() {
    // Revoke granted permissions
    for (PermissionGrant grant : grantedPermissions) {
      try {
        LOGGER.debug(
            "Revoking permissions {} from {} on {}",
            grant.privileges,
            grant.userEmail,
            grant.resourceFullName);
        PermissionsChange permissionsChange =
            new PermissionsChange()
                .principal(grant.userEmail)
                .add(List.of())
                .remove(grant.privileges);
        UpdatePermissions updatePermissions =
            new UpdatePermissions().changes(java.util.List.of(permissionsChange));
        grantsApi.update(grant.resourceSecurableType, grant.resourceFullName, updatePermissions);
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to revoke permissions for {} on {}: {}",
            grant.userEmail,
            grant.resourceFullName,
            e.getMessage());
      }
    }

    // Delete created users
    for (String userId : createdUserIds) {
      try {
        LOGGER.debug("Deleting test user: {}", userId);
        usersApi.deleteUser(userId);
      } catch (Exception e) {
        LOGGER.warn("Failed to delete test user {}: {}", userId, e.getMessage());
      }
    }

    // Clear tracking lists for each test
    createdUserIds.clear();
    grantedPermissions.clear();
  }

  /** Creates common resources (catalog and schema) using an admin token for authorization. */
  private void createCommonResources() {
    CatalogOperations adminCatalogOps =
        new SdkCatalogOperations(TestUtils.createApiClient(adminConfig));
    SchemaOperations adminSchemaOps =
        new SdkSchemaOperations(TestUtils.createApiClient(adminConfig));

    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    try {
      adminCatalogOps.createCatalog(createCatalog);
      adminSchemaOps.createSchema(
          new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a control API client for user management operations.
   *
   * @param config Server configuration including URL and auth token
   * @return A configured control API client
   */
  protected io.unitycatalog.control.ApiClient createControlApiClient(ServerConfig config) {
    io.unitycatalog.control.ApiClient controlApiClient = new io.unitycatalog.control.ApiClient();
    java.net.URI uri = java.net.URI.create(config.getServerUrl());
    int port = uri.getPort();
    controlApiClient.setHost(uri.getHost());
    controlApiClient.setPort(port);
    controlApiClient.setScheme(uri.getScheme());
    if (config.getAuthToken() != null && !config.getAuthToken().isEmpty()) {
      controlApiClient.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + config.getAuthToken()));
    }
    return controlApiClient;
  }

  /**
   * Creates a test user in the database. This is required before the user can be authenticated. The
   * created user is tracked and will be automatically deleted during tearDown.
   *
   * @param email The user's email
   * @param name The user's display name
   * @return The created User object
   * @throws io.unitycatalog.control.ApiException if user creation fails
   */
  protected UserResource createTestUser(String email, String name)
      throws io.unitycatalog.control.ApiException {
    UserResource userResource =
        new UserResource()
            .displayName(name)
            .emails(java.util.List.of(new Email().value(email).primary(true)));
    UserResource createdUser = usersApi.createUser(userResource);
    // Track the created user for cleanup
    createdUserIds.add(createdUser.getId());
    LOGGER.debug("Created test user: {} (ID: {})", email, createdUser.getId());
    return createdUser;
  }

  /**
   * Grants permissions to a user on a resource. The granted permissions are tracked and will be
   * automatically revoked during tearDown.
   *
   * @param userEmail The user's email
   * @param resourceSecurableType The resource securable type
   * @param resourceFullName The resource full name
   * @param privileges The privileges to grant
   * @throws Exception if permission grant fails
   */
  protected void grantPermissions(
      String userEmail,
      SecurableType resourceSecurableType,
      String resourceFullName,
      Privileges... privileges)
      throws Exception {
    LOGGER.debug("grantPermissions {} {} {}", userEmail, resourceFullName, privileges);
    // Build list of Privilege enums from Privileges
    List<Privilege> privilegeList =
        Arrays.stream(privileges)
            .map(p -> Privilege.fromValue(p.getValue()))
            .collect(Collectors.toList());
    if (!privilegeList.isEmpty()) {
      PermissionsChange permissionsChange =
          new PermissionsChange().principal(userEmail).add(privilegeList).remove(List.of());
      UpdatePermissions updatePermissions =
          new UpdatePermissions().changes(java.util.List.of(permissionsChange));
      grantsApi.update(resourceSecurableType, resourceFullName, updatePermissions);
      // Track the granted permissions for cleanup
      grantedPermissions.add(
          new PermissionGrant(userEmail, resourceSecurableType, resourceFullName, privilegeList));
    }
  }

  /**
   * Helper method to create a JWT token for testing purposes using the actual RSA keys from the
   * configuration.
   *
   * <p>This method creates tokens the same way the production system does, using RSA512 algorithm
   * with the keys from etc/conf directory.
   *
   * @param subject The subject (email) for the JWT token
   * @return A JWT token string signed with the actual RSA private key
   */
  protected String createTestJwtToken(String subject) {
    try {
      // Use the securityConfiguration that was set up by BaseAccessControlCRUDTest
      Algorithm algorithm = securityConfiguration.algorithmRSA();
      String keyId = securityConfiguration.getKeyId();

      // Create token following the same pattern as SecurityContext.createAccessToken
      return JWT.create()
          .withSubject(subject)
          .withIssuer(INTERNAL)
          .withIssuedAt(new Date())
          .withKeyId(keyId)
          .withJWTId(UUID.randomUUID().toString())
          .withClaim("email", subject)
          .sign(algorithm);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test JWT token: " + e.getMessage(), e);
    }
  }

  /**
   * Helper method to create a ServerConfig for the user.
   *
   * @param subject The subject (email) for the user
   * @return A ServerConfig with the user token
   */
  protected ServerConfig createTestUserServerConfig(String subject) {
    return new ServerConfig(serverConfig.getServerUrl(), createTestJwtToken(subject));
  }
}
