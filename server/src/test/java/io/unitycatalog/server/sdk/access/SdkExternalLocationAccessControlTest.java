package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.ListExternalLocationsResponse;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
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

  private Set<String> externalLocationsToDelete = new HashSet<>();

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
  @Override
  @AfterEach
  public void tearDown() {
    Thread.sleep(5000);
    for (String externalLocationName : externalLocationsToDelete) {
      try {
        adminApi.deleteExternalLocation(externalLocationName, true);
      } catch (ApiException e) {
        // Already deleted
      }
    }
    adminCredentialsApi.deleteCredential(CREDENTIAL_NAME, true);
    adminCredentialsApi.deleteCredential(ANOTHER_CREDENTIAL_NAME, true);
    super.tearDown();
  }

  @SneakyThrows
  private ExternalLocationsApi createExternalLocationsApi(String email) {
    createTestUser(email, email.split("@")[0]);
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
    externalLocationsToDelete.add(name);
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
    assertApiException(
        () -> api.createExternalLocation(createRequest(name)),
        ErrorCode.PERMISSION_DENIED,
        "Access denied.");
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
      assertApiException(
          () -> api.getExternalLocation(name), ErrorCode.PERMISSION_DENIED, "Access denied");
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
    assertApiException(
        () ->
            api.updateExternalLocation(
                name, new UpdateExternalLocation().url("s3://test-bucket/fail-path-" + name)),
        ErrorCode.PERMISSION_DENIED,
        "Access denied");
  }

  private void assertUpdateCredentialFailure(
      ExternalLocationsApi api, String name, String newCredentialName) {
    assertApiException(
        () ->
            api.updateExternalLocation(
                name, new UpdateExternalLocation().credentialName(newCredentialName)),
        ErrorCode.PERMISSION_DENIED,
        "Access denied");
  }

  private void assertDeleteSuccess(ExternalLocationsApi api, String name) throws ApiException {
    api.deleteExternalLocation(name, false);
    externalLocationsToDelete.remove(name);
    // Verify deletion by checking that get fails with 404
    assertApiException(() -> adminApi.getExternalLocation(name), ErrorCode.NOT_FOUND, "not found");
  }

  private void assertDeleteFailure(ExternalLocationsApi api, String name) {
    assertApiException(
        () -> api.deleteExternalLocation(name, false),
        ErrorCode.PERMISSION_DENIED,
        "Access denied");
  }
}
