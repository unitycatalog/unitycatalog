package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.ListExternalLocationsResponse;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SdkExternalLocationAccessControlTest extends SdkAccessControlBaseCRUDTest {

  private static final String CREDENTIAL_NAME = "test_credential";
  private static final String ANOTHER_CREDENTIAL_NAME = "another_test_credential";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String URL_TEMPLATE = "s3://test-bucket/path-%s";

  // Test users
  private static final String USER_A_EMAIL = "userA@example.com";
  private static final String USER_B_EMAIL = "userB@example.com";
  private static final String USER_C_EMAIL = "userC@example.com";

  // User API clients
  private CredentialsApi adminCredentialsApi;
  private ExternalLocationsApi adminApi;
  private ExternalLocationsApi userAApi;
  private ExternalLocationsApi userBApi;
  private ExternalLocationsApi userCApi;

  @SneakyThrows
  @Override
  @BeforeEach
  public void setUp() {
    super.setUp();
    adminCredentialsApi = new CredentialsApi(adminApiClient);

    // Create two credentials as admin
    for (String name : List.of(CREDENTIAL_NAME, ANOTHER_CREDENTIAL_NAME)) {
      adminCredentialsApi.createCredential(
          new CreateCredentialRequest()
              .name(name)
              .purpose(CredentialPurpose.STORAGE)
              .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)));
    }

    // Set up users with different permission combinations

    // Admin is already set up by base class and is owner of metastore
    adminApi = new ExternalLocationsApi(adminApiClient);

    // UserA: Has CREATE_EXTERNAL_LOCATION on metastore only
    userAApi = createExternalLocationsApi(USER_A_EMAIL);
    grantPermissions(
        USER_A_EMAIL, SecurableType.METASTORE, METASTORE_NAME, Privileges.CREATE_EXTERNAL_LOCATION);

    // UserB: Has CREATE_EXTERNAL_LOCATION on credential only
    userBApi = createExternalLocationsApi(USER_B_EMAIL);
    grantPermissions(
        USER_B_EMAIL,
        SecurableType.CREDENTIAL,
        CREDENTIAL_NAME,
        Privileges.CREATE_EXTERNAL_LOCATION);

    // UserC: Has CREATE_EXTERNAL_LOCATION on both metastore and credential
    userCApi = createExternalLocationsApi(USER_C_EMAIL);
    grantPermissions(
        USER_C_EMAIL, SecurableType.METASTORE, METASTORE_NAME, Privileges.CREATE_EXTERNAL_LOCATION);
    grantPermissions(
        USER_C_EMAIL,
        SecurableType.CREDENTIAL,
        CREDENTIAL_NAME,
        Privileges.CREATE_EXTERNAL_LOCATION);
  }

  @SneakyThrows
  private ExternalLocationsApi createExternalLocationsApi(String email) {
    createTestUser(email);
    return new ExternalLocationsApi(TestUtils.createApiClient(createTestUserServerConfig(email)));
  }

  /**
   * Test create operation permissions. Only admin (metastore owner) and userC (who has permissions
   * on both metastore and credential) should be able to create external locations.
   */
  @Test
  public void testExternalLocationPermissions() throws Exception {
    // Test creation

    // Admin can create (metastore owner)
    String adminLocation = "admin_location";
    assertCreateSuccess(adminApi, adminLocation);

    // UserA cannot create (has metastore permission but no credential permission)
    String userALocation = "userA_location";
    assertCreateFailure(userAApi, userALocation);

    // UserB cannot create (has credential permission but no permission on metastore)
    String userBLocation = "userB_location";
    assertCreateFailure(userBApi, userBLocation);

    // UserC can create (has permission on both metastore and credential).
    String userCLocation = "userC_location";
    String userCLocation2 = "userC_location2";
    assertCreateSuccess(userCApi, userCLocation);
    assertCreateSuccess(userCApi, userCLocation2);

    // Test update operations

    // Admin can update its own external location
    assertUpdateUrlSuccess(adminApi, adminLocation, "s3://test-bucket/another-admin-path1");

    // Admin can always update userC's external location (metastore owner)
    assertUpdateUrlSuccess(adminApi, userCLocation, "s3://test-bucket/another-admin-path2");
    assertUpdateCredentialSuccess(adminApi, userCLocation, ANOTHER_CREDENTIAL_NAME);

    // UserC can update its own external location (owner), but not if it tries to use another
    // credential without permission.
    assertUpdateUrlSuccess(userCApi, userCLocation, "s3://test-bucket/another-userc-path1");
    assertUpdateCredentialFailure(userCApi, userCLocation, ANOTHER_CREDENTIAL_NAME);

    // UserC cannot update admin's external location (not owner, not metastore owner)
    assertUpdateUrlFailure(userCApi, adminLocation);

    // UserA and UserB cannot update any external location (no ownership)
    assertUpdateUrlFailure(userAApi, adminLocation);
    assertUpdateUrlFailure(userAApi, userCLocation);
    assertUpdateUrlFailure(userBApi, adminLocation);
    assertUpdateUrlFailure(userBApi, userCLocation);

    // Test get and list operations

    // Admin can get and list all
    assertGetAndListPermissions(
        adminApi, List.of(adminLocation, userCLocation, userCLocation2), List.of());

    // User A and B can get and list neither
    assertGetAndListPermissions(
        userAApi, List.of(), List.of(adminLocation, userCLocation, userCLocation2));
    assertGetAndListPermissions(
        userBApi, List.of(), List.of(adminLocation, userCLocation, userCLocation2));

    // User C can only get and list its own external locations (owner)
    assertGetAndListPermissions(
        userCApi, List.of(userCLocation, userCLocation2), List.of(adminLocation));

    // Grant user A and B any permission so that they can get and list too.
    grantPermissions(
        USER_A_EMAIL, SecurableType.EXTERNAL_LOCATION, adminLocation, Privileges.READ_FILES);
    assertGetAndListPermissions(
        userAApi, List.of(adminLocation), List.of(userCLocation, userCLocation2));

    grantPermissions(
        USER_B_EMAIL,
        SecurableType.EXTERNAL_LOCATION,
        userCLocation,
        Privileges.CREATE_MANAGED_STORAGE);
    assertGetAndListPermissions(
        userBApi, List.of(userCLocation), List.of(adminLocation, userCLocation2));

    // Test deletion operation

    // User A and B can not delete any external location
    assertDeleteFailure(userAApi, adminLocation);
    assertDeleteFailure(userAApi, userCLocation);
    assertDeleteFailure(userBApi, adminLocation);
    assertDeleteFailure(userBApi, userCLocation);

    // User C can delete its own external location
    assertDeleteFailure(userCApi, adminLocation);
    assertDeleteSuccess(userCApi, userCLocation);

    // Admin can delete any external location
    assertDeleteSuccess(adminApi, userCLocation2);
    assertDeleteSuccess(adminApi, adminLocation);
  }

  private void assertCreateSuccess(ExternalLocationsApi api, String name) throws ApiException {
    ExternalLocationInfo created = api.createExternalLocation(createRequest(name));
    assertThat(created.getName()).isEqualTo(name);
    assertThat(created.getUrl()).isNotNull();
    assertThat(created.getId()).isNotNull();
  }

  private CreateExternalLocation createRequest(String name) {
    return new CreateExternalLocation()
        .name(name)
        .url(String.format(URL_TEMPLATE, name))
        .credentialName(CREDENTIAL_NAME);
  }

  private void assertCreateFailure(ExternalLocationsApi api, String name) {
    assertPermissionDenied(() -> api.createExternalLocation(createRequest(name)));
  }

  @SneakyThrows
  private void assertGetAndListPermissions(
      ExternalLocationsApi api,
      List<String> allowedExternalLocationNames,
      List<String> deniedExternalLocationNames) {
    // Test get operation
    for (String name : allowedExternalLocationNames) {
      ExternalLocationInfo retrieved = api.getExternalLocation(name);
      assertThat(retrieved.getName()).isEqualTo(name);
      assertThat(retrieved.getId()).isNotNull().isNotEmpty();
      assertThat(retrieved.getUrl()).isNotNull().isNotEmpty();
    }
    for (String name : deniedExternalLocationNames) {
      assertPermissionDenied(() -> api.getExternalLocation(name));
    }
    // Test list operation
    ListExternalLocationsResponse response = api.listExternalLocations(100, null);
    assertThat(response.getNextPageToken()).isNull();
    response.getExternalLocations().forEach(el -> assertThat(el.getId()).isNotNull().isNotEmpty());
    response.getExternalLocations().forEach(el -> assertThat(el.getUrl()).isNotNull().isNotEmpty());
    assertThat(response.getExternalLocations().stream().map(ExternalLocationInfo::getName))
        .containsExactlyInAnyOrderElementsOf(allowedExternalLocationNames);
  }

  private void assertUpdateUrlSuccess(ExternalLocationsApi api, String name, String url)
      throws ApiException {
    ExternalLocationInfo updated =
        api.updateExternalLocation(name, new UpdateExternalLocation().url(url));
    assertThat(updated.getUrl()).isEqualTo(url);
  }

  private void assertUpdateCredentialSuccess(
      ExternalLocationsApi api, String name, String newCredentialName) throws ApiException {
    ExternalLocationInfo updated =
        api.updateExternalLocation(
            name, new UpdateExternalLocation().credentialName(newCredentialName));
    assertThat(updated.getCredentialName()).isEqualTo(newCredentialName);
  }

  private void assertUpdateUrlFailure(ExternalLocationsApi api, String name) {
    assertPermissionDenied(
        () ->
            api.updateExternalLocation(
                name, new UpdateExternalLocation().url("s3://test-bucket/fail-path-" + name)));
  }

  private void assertUpdateCredentialFailure(
      ExternalLocationsApi api, String name, String newCredentialName) {
    assertPermissionDenied(
        () ->
            api.updateExternalLocation(
                name, new UpdateExternalLocation().credentialName(newCredentialName)));
  }

  private void assertDeleteSuccess(ExternalLocationsApi api, String name) throws ApiException {
    api.deleteExternalLocation(name, false);
    // Verify deletion by checking that get fails with 404
    assertApiException(() -> adminApi.getExternalLocation(name), ErrorCode.NOT_FOUND, "not found");
  }

  private void assertDeleteFailure(ExternalLocationsApi api, String name) {
    assertPermissionDenied(() -> api.deleteExternalLocation(name, false));
  }

  /**
   * Test case configuration for external table and volume creation permission tests.
   *
   * <p>Each test case defines a user email, optional storage root, permissions to grant, and
   * expected outcomes for table and volume creation attempts.
   */
  @Builder
  @Getter
  public static class CreateTableVolumeTestCase {
    /** The email address of the test user. */
    private final String email;

    /**
     * Optional storage root URL for the test. If null, the external location URL is used. Setting
     * this to a non-registered path tests the scenario where no external location covers the path.
     */
    private final String storageRoot;

    /** List of privileges to grant to the test user. */
    @Singular private final List<Privileges> grantPermissions;

    /** Whether the user is expected to be able to create an external table. */
    private final boolean expectCanCreateExternalTable;

    /** Whether the user is expected to be able to create an external volume. */
    private final boolean expectCanCreateExternalVolume;
  }

  /**
   * Test that creating external tables and volumes requires appropriate permissions on external
   * locations.
   */
  @Test
  public void testCreateExternalTableVolumePermissions() throws Exception {
    // Create external location as userC
    String externalLocationName = "test_external_loc";
    String extLocationUrl = "s3://ext-bucket/tables";
    userCApi.createExternalLocation(
        new CreateExternalLocation()
            .name(externalLocationName)
            .url(extLocationUrl)
            .credentialName(CREDENTIAL_NAME));

    List<CreateTableVolumeTestCase> testCases =
        List.of(
            // A user can't create table or volume without permission
            CreateTableVolumeTestCase.builder().email("no_permission@example.com").build(),
            // A user can't create table or volume with only permission on schema
            CreateTableVolumeTestCase.builder()
                .email("only_schema_perm@example.com")
                .grantPermission(Privileges.CREATE_TABLE)
                .grantPermission(Privileges.CREATE_VOLUME)
                .build(),
            // A user can create table or volume with only permission on schema if it's not using
            // any external location (path is not registered)
            CreateTableVolumeTestCase.builder()
                .email("only_schema_perm_use_no_external_location@example.com")
                .storageRoot("s3://some-other-unregistered-bucket/root")
                .grantPermission(Privileges.CREATE_TABLE)
                .grantPermission(Privileges.CREATE_VOLUME)
                .expectCanCreateExternalTable(true)
                .expectCanCreateExternalVolume(true)
                .build(),
            // A user can't create table or volume with only permission on location
            CreateTableVolumeTestCase.builder()
                .email("only_location_perm@example.com")
                .grantPermission(Privileges.CREATE_EXTERNAL_TABLE)
                .grantPermission(Privileges.CREATE_EXTERNAL_VOLUME)
                .build(),
            // A user can create table with both permissions on schema and location
            CreateTableVolumeTestCase.builder()
                .email("only_table@example.com")
                .grantPermission(Privileges.CREATE_EXTERNAL_TABLE)
                .grantPermission(Privileges.CREATE_TABLE)
                .expectCanCreateExternalTable(true)
                .build(),
            // A user can create volume with both permissions on schema and location
            CreateTableVolumeTestCase.builder()
                .email("only_volume@example.com")
                .grantPermission(Privileges.CREATE_EXTERNAL_VOLUME)
                .grantPermission(Privileges.CREATE_VOLUME)
                .expectCanCreateExternalVolume(true)
                .build(),
            // A user can create both table and volume with all 4 permissions
            CreateTableVolumeTestCase.builder()
                .email("table_and_volume@example.com")
                .grantPermission(Privileges.CREATE_EXTERNAL_TABLE)
                .grantPermission(Privileges.CREATE_EXTERNAL_VOLUME)
                .grantPermission(Privileges.CREATE_TABLE)
                .grantPermission(Privileges.CREATE_VOLUME)
                .expectCanCreateExternalVolume(true)
                .expectCanCreateExternalTable(true)
                .build(),
            // Location owner can create as long as it has the proper schema permissions
            CreateTableVolumeTestCase.builder()
                .email(USER_C_EMAIL)
                .grantPermission(Privileges.CREATE_TABLE)
                .grantPermission(Privileges.CREATE_VOLUME)
                .expectCanCreateExternalTable(true)
                .expectCanCreateExternalVolume(true)
                .build());

    int counter = 0;
    for (CreateTableVolumeTestCase testCase : testCases) {
      counter++;
      String storageRoot =
          testCase.getStorageRoot() != null ? testCase.getStorageRoot() : extLocationUrl;

      // Skip creating user if it's a known user
      if (!testCase.email.equals(USER_C_EMAIL)) {
        createTestUser(testCase.email);
      }

      // Grant common permissions on catalog and schema
      grantPermissions(
          testCase.email, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
      grantPermissions(
          testCase.email, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
      // Grant test case specific permissions
      for (Privileges privilege : testCase.grantPermissions) {
        switch (privilege) {
          case CREATE_TABLE, CREATE_VOLUME -> grantPermissions(
              testCase.email, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, privilege);
          case CREATE_EXTERNAL_TABLE, CREATE_EXTERNAL_VOLUME -> grantPermissions(
              testCase.email, SecurableType.EXTERNAL_LOCATION, externalLocationName, privilege);
          default -> throw new RuntimeException("Unknown privilege: " + privilege);
        }
      }

      ApiClient apiClient = TestUtils.createApiClient(createTestUserServerConfig(testCase.email));

      // Verify that the Table creation is as expected
      String tableName = TestUtils.TABLE_NAME + counter;
      String tableLocation = storageRoot + "/" + tableName;
      TablesApi tablesApi = new TablesApi(apiClient);
      if (testCase.expectCanCreateExternalTable) {
        TableInfo tableInfo =
            tablesApi.createTable(createExternalTableRequest(tableName, tableLocation));
        assertThat(tableInfo).isNotNull();
        assertThat(tableInfo.getStorageLocation()).startsWith(storageRoot);

        // Verify that we can not create another under the same path, or subdir, or parent
        for (String url : List.of(tableLocation, tableLocation + "/subdir", storageRoot)) {
          assertPermissionDenied(
              () -> tablesApi.createTable(createExternalTableRequest(tableName + "_another", url)));
        }
      } else {
        assertPermissionDenied(
            () -> tablesApi.createTable(createExternalTableRequest(tableName, tableLocation)));
      }

      // Verify that the Volume creation is as expected
      String volumeName = TestUtils.VOLUME_NAME + counter;
      String volumeLocation = storageRoot + "/" + volumeName;
      VolumesApi volumesApi = new VolumesApi(apiClient);
      if (testCase.expectCanCreateExternalVolume) {
        VolumeInfo volumeInfo =
            volumesApi.createVolume(createExternalVolumeRequest(volumeName, volumeLocation));
        assertThat(volumeInfo).isNotNull();
        assertThat(volumeInfo.getStorageLocation()).startsWith(storageRoot);

        // Verify that we can not create another under the same path, or subdir, or parent
        for (String url : List.of(volumeLocation, volumeLocation + "/subdir", storageRoot)) {
          assertPermissionDenied(
              () ->
                  volumesApi.createVolume(
                      createExternalVolumeRequest(volumeName + "_another", url)));
        }
      } else {
        assertPermissionDenied(
            () -> volumesApi.createVolume(createExternalVolumeRequest(volumeName, volumeLocation)));
      }

      // Verify that creation of a table at storage location of an existing volume should fail,
      // vice versa.
      if (testCase.expectCanCreateExternalTable && testCase.expectCanCreateExternalVolume) {
        assertPermissionDenied(
            () ->
                volumesApi.createVolume(
                    createExternalVolumeRequest(volumeName + "_another", tableLocation)));
        assertPermissionDenied(
            () ->
                tablesApi.createTable(
                    createExternalTableRequest(tableName + "_another", volumeLocation)));
      }
    }
  }

  private CreateTable createExternalTableRequest(String name, String storageLocation) {
    return new CreateTable()
        .name(name)
        .catalogName(TestUtils.CATALOG_NAME)
        .schemaName(TestUtils.SCHEMA_NAME)
        .tableType(TableType.EXTERNAL)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .storageLocation(storageLocation)
        .columns(
            List.of(
                new ColumnInfo()
                    .name("id")
                    .typeName(ColumnTypeName.INT)
                    .typeText("INTEGER")
                    .typeJson("{\"type\": \"integer\"}")
                    .position(0)
                    .nullable(true)));
  }

  private CreateVolumeRequestContent createExternalVolumeRequest(
      String name, String storageLocation) {
    return new CreateVolumeRequestContent()
        .name(name)
        .catalogName(TestUtils.CATALOG_NAME)
        .schemaName(TestUtils.SCHEMA_NAME)
        .volumeType(VolumeType.EXTERNAL)
        .storageLocation(storageLocation);
  }
}
