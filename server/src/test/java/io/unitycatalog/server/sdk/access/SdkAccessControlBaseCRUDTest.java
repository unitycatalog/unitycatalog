package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.FunctionsApi;
import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.api.ModelVersionsApi;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.FunctionInfo;
import io.unitycatalog.client.model.ListFunctionsResponse;
import io.unitycatalog.client.model.ListModelVersionsResponse;
import io.unitycatalog.client.model.ListRegisteredModelsResponse;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.ListVolumesResponseContent;
import io.unitycatalog.client.model.ModelVersionInfo;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.RegisteredModelInfo;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.client.model.VolumeInfo;
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
import lombok.SneakyThrows;
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

  // Common test user email addresses
  protected static final String PRINCIPAL_1 = "principal-1@localhost";
  protected static final String PRINCIPAL_2 = "principal-2@localhost";
  protected static final String REGULAR_1 = "regular-1@localhost";
  protected static final String REGULAR_2 = "regular-2@localhost";

  protected ServerConfig adminConfig;
  protected UsersApi usersApi;
  protected GrantsApi grantsApi;
  protected ApiClient adminApiClient;

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
    adminApiClient = TestUtils.createApiClient(adminConfig);
    grantsApi = new GrantsApi(adminApiClient);
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
    super.tearDown();
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

  protected UserResource createTestUser(String email) throws io.unitycatalog.control.ApiException {
    return createTestUser(email, email.split("@")[0]);
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

  /**
   * Helper method to list all tables with pagination.
   *
   * @param tablesApi The TablesApi instance
   * @param catalogName The catalog name
   * @param schemaName The schema name
   * @return List of all tables
   */
  @SneakyThrows
  protected List<TableInfo> listAllTables(
      TablesApi tablesApi, String catalogName, String schemaName) {
    List<TableInfo> allTables = new ArrayList<>();
    String pageToken = null;
    do {
      ListTablesResponse response = tablesApi.listTables(catalogName, schemaName, 100, pageToken);
      allTables.addAll(response.getTables());
      pageToken = response.getNextPageToken();
    } while (pageToken != null);
    return allTables;
  }

  /**
   * Helper method to list all volumes with pagination.
   *
   * @param volumesApi The VolumesApi instance
   * @param catalogName The catalog name
   * @param schemaName The schema name
   * @return List of all volumes
   */
  @SneakyThrows
  protected List<VolumeInfo> listAllVolumes(
      VolumesApi volumesApi, String catalogName, String schemaName) {
    List<VolumeInfo> allVolumes = new ArrayList<>();
    String pageToken = null;
    do {
      ListVolumesResponseContent response =
          volumesApi.listVolumes(catalogName, schemaName, 100, pageToken);
      allVolumes.addAll(response.getVolumes());
      pageToken = response.getNextPageToken();
    } while (pageToken != null);
    return allVolumes;
  }

  /**
   * Helper method to list all functions with pagination.
   *
   * @param functionsApi The FunctionsApi instance
   * @param catalogName The catalog name
   * @param schemaName The schema name
   * @return List of all functions
   */
  @SneakyThrows
  protected List<FunctionInfo> listAllFunctions(
      FunctionsApi functionsApi, String catalogName, String schemaName) {
    List<FunctionInfo> allFunctions = new ArrayList<>();
    String pageToken = null;
    do {
      ListFunctionsResponse response =
          functionsApi.listFunctions(catalogName, schemaName, 100, pageToken);
      allFunctions.addAll(response.getFunctions());
      pageToken = response.getNextPageToken();
    } while (pageToken != null);
    return allFunctions;
  }

  /**
   * Helper method to list all registered models with pagination.
   *
   * @param modelsApi The RegisteredModelsApi instance
   * @param catalogName The catalog name
   * @param schemaName The schema name
   * @return List of all registered models
   */
  @SneakyThrows
  protected List<RegisteredModelInfo> listAllRegisteredModels(
      RegisteredModelsApi modelsApi, String catalogName, String schemaName) {
    List<RegisteredModelInfo> allModels = new ArrayList<>();
    String pageToken = null;
    do {
      ListRegisteredModelsResponse response =
          modelsApi.listRegisteredModels(catalogName, schemaName, 100, pageToken);
      allModels.addAll(response.getRegisteredModels());
      pageToken = response.getNextPageToken();
    } while (pageToken != null);
    return allModels;
  }

  /**
   * Helper method to list all model versions with pagination.
   *
   * @param versionsApi The ModelVersionsApi instance
   * @param fullName The full model name
   * @return List of all model versions
   */
  @SneakyThrows
  protected List<ModelVersionInfo> listAllModelVersions(
      ModelVersionsApi versionsApi, String fullName) {
    List<ModelVersionInfo> allVersions = new ArrayList<>();
    String pageToken = null;
    do {
      ListModelVersionsResponse response = versionsApi.listModelVersions(fullName, 100, pageToken);
      allVersions.addAll(response.getModelVersions());
      pageToken = response.getNextPageToken();
    } while (pageToken != null);
    return allVersions;
  }

  /**
   * Helper method to get a table with default parameters.
   *
   * @param tablesApi The TablesApi instance
   * @param tableFullName The full table name
   * @return The table info
   */
  @SneakyThrows
  protected TableInfo getTable(TablesApi tablesApi, String tableFullName) {
    return tablesApi.getTable(tableFullName, true, false);
  }

  /** Common setup: Create standard test users (principal-1, principal-2, regular-1, regular-2). */
  @SneakyThrows
  protected void createCommonTestUsers() {
    createTestUser(PRINCIPAL_1, "Principal 1");
    createTestUser(PRINCIPAL_2, "Principal 2");
    createTestUser(REGULAR_1, "Regular 1");
    createTestUser(REGULAR_2, "Regular 2");
  }

  /**
   * Common setup: Creates catalog "cat_pr1" with schema "sch_pr1". Equivalent to
   * commonSecurableSteps in CLI tests. This setup includes: - Grant CREATE CATALOG to principal-1 -
   * Create catalog "cat_pr1" as principal-1 - Grant CREATE SCHEMA and USE CATALOG to principal-1 -
   * Create schema "sch_pr1" in cat_pr1
   */
  @SneakyThrows
  protected void setupCommonCatalogAndSchema() {
    ServerConfig principal1Config = createTestUserServerConfig(PRINCIPAL_1);
    io.unitycatalog.client.api.CatalogsApi principal1CatalogsApi =
        new io.unitycatalog.client.api.CatalogsApi(TestUtils.createApiClient(principal1Config));
    io.unitycatalog.client.api.SchemasApi principal1SchemasApi =
        new io.unitycatalog.client.api.SchemasApi(TestUtils.createApiClient(principal1Config));

    // give user CREATE CATALOG
    grantPermissions(
        PRINCIPAL_1, SecurableType.METASTORE, METASTORE_NAME, Privileges.CREATE_CATALOG);

    // create a catalog -> CREATE CATALOG -> allowed
    io.unitycatalog.client.model.CreateCatalog createCatalog =
        new io.unitycatalog.client.model.CreateCatalog()
            .name("cat_pr1")
            .comment("(created from scratch)");
    principal1CatalogsApi.createCatalog(createCatalog);

    // give user CREATE SCHEMA on cat_pr1
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, "cat_pr1", Privileges.CREATE_SCHEMA);
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);

    // create schema
    io.unitycatalog.client.model.CreateSchema createSchema =
        new io.unitycatalog.client.model.CreateSchema().name("sch_pr1").catalogName("cat_pr1");
    principal1SchemasApi.createSchema(createSchema);
  }

  protected static final List<ColumnInfo> TEST_COLUMNS =
      List.of(
          new ColumnInfo()
              .name("id")
              .typeText("INT")
              .typeJson("{\"type\": \"integer\"}")
              .typeName(ColumnTypeName.INT)
              .position(0)
              .nullable(true));

  /**
   * Creates a credential and external location for managed storage testing.
   *
   * @param credentialName The name for the credential
   * @param externalLocationName The name for the external location
   * @param locationUrl The URL for the external location
   */
  @SneakyThrows
  protected void createExternalLocationWithCredential(
      String credentialName, String externalLocationName, String locationUrl) {
    CredentialsApi credentialsApi = new CredentialsApi(adminApiClient);
    CreateCredentialRequest createCredential =
        new CreateCredentialRequest()
            .name(credentialName)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn("fake_arn"));
    credentialsApi.createCredential(createCredential);

    ExternalLocationsApi externalLocationsApi = new ExternalLocationsApi(adminApiClient);
    CreateExternalLocation createExternalLocation =
        new CreateExternalLocation()
            .name(externalLocationName)
            .url(locationUrl)
            .credentialName(credentialName);
    externalLocationsApi.createExternalLocation(createExternalLocation);
  }

  /**
   * Creates an external table with the specified parameters.
   *
   * @param tablesApi The TablesApi instance
   * @param catalogName The catalog name
   * @param schemaName The schema name
   * @param tableName The table name
   * @param storageLocation The storage location
   * @return The created TableInfo
   */
  @SneakyThrows
  protected TableInfo createExternalTable(
      TablesApi tablesApi,
      String catalogName,
      String schemaName,
      String tableName,
      String storageLocation) {
    CreateTable createTable =
        new CreateTable()
            .name(tableName)
            .catalogName(catalogName)
            .schemaName(schemaName)
            .columns(TEST_COLUMNS)
            .storageLocation(storageLocation)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA);
    return tablesApi.createTable(createTable);
  }
}
