package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.ListCredentialsResponse;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdateCredentialRequest;
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

public class SdkCredentialAccessControlTest extends SdkAccessControlBaseCRUDTest {

  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";

  // Test users
  private static final String USER_A_EMAIL = "userA@example.com";
  private static final String USER_B_EMAIL = "userB@example.com";
  private static final String USER_C_EMAIL = "userC@example.com";

  // User API clients
  private CredentialsApi adminApi;
  private CredentialsApi userAApi;
  private CredentialsApi userBApi;
  private CredentialsApi userCApi;

  private Set<String> credentialsToDelete = new HashSet<>();
  private int updateCounter = 0;

  @SneakyThrows
  @Override
  @BeforeEach
  public void setUp() {
    super.setUp();

    // Set up users with different permission combinations

    // Admin is already set up by base class and is owner of metastore
    adminApi = new CredentialsApi(adminApiClient);

    // UserA: Has CREATE_STORAGE_CREDENTIAL on metastore
    userAApi = createCredentialsApi(USER_A_EMAIL);
    grantPermissions(
        USER_A_EMAIL,
        SecurableType.METASTORE,
        METASTORE_NAME,
        Privileges.CREATE_STORAGE_CREDENTIAL);

    // UserB: Will be granted CREATE_EXTERNAL_LOCATION on a specific credential later
    userBApi = createCredentialsApi(USER_B_EMAIL);

    // UserC: Has no permissions
    userCApi = createCredentialsApi(USER_C_EMAIL);
  }

  @SneakyThrows
  @Override
  @AfterEach
  public void tearDown() {
    for (String credentialName : credentialsToDelete) {
      try {
        adminApi.deleteCredential(credentialName, true);
      } catch (ApiException e) {
        // Already deleted
      }
    }
    super.tearDown();
  }

  @SneakyThrows
  private CredentialsApi createCredentialsApi(String email) {
    createTestUser(email, email.split("@")[0]);
    return new CredentialsApi(TestUtils.createApiClient(createTestUserServerConfig(email)));
  }

  /**
   * Test credential access control:
   *
   * <ul>
   *   <li>Admin (metastore owner) can create, update, delete all credentials
   *   <li>UserA (has CREATE_STORAGE_CREDENTIAL) can create credentials and update/delete only their
   *       own
   *   <li>UserB (has CREATE_EXTERNAL_LOCATION on one credential) can only GET/LIST that credential
   *   <li>UserC (no permissions) cannot do anything
   * </ul>
   */
  @Test
  public void testCredentialPermissions() throws Exception {
    // Test creation

    // Admin can create (metastore owner)
    String adminCredential = "admin_credential";
    assertCreateSuccess(adminApi, adminCredential);

    // UserA can create (has CREATE_STORAGE_CREDENTIAL permission)
    String userACredential1 = "userA_credential1";
    String userACredential2 = "userA_credential2";
    assertCreateSuccess(userAApi, userACredential1);
    assertCreateSuccess(userAApi, userACredential2);

    // UserB cannot create (no CREATE_STORAGE_CREDENTIAL permission)
    String userBCredential = "userB_credential";
    assertCreateFailure(userBApi, userBCredential);

    // UserC cannot create (no permissions)
    String userCCredential = "userC_credential";
    assertCreateFailure(userCApi, userCCredential);

    // Grant UserB CREATE_EXTERNAL_LOCATION permission on userACredential1
    grantPermissions(
        USER_B_EMAIL,
        SecurableType.CREDENTIAL,
        userACredential1,
        Privileges.CREATE_EXTERNAL_LOCATION);

    // Test update operations

    // Admin can update its own credential
    assertUpdateSuccess(adminApi, adminCredential);

    // Admin can update userA's credentials (metastore owner)
    assertUpdateSuccess(adminApi, userACredential1);

    // UserA can update its own credentials (owner)
    assertUpdateSuccess(userAApi, userACredential1);
    assertUpdateSuccess(userAApi, userACredential2);

    // UserA cannot update admin's credential (not owner, not metastore owner)
    assertUpdateFailure(userAApi, adminCredential);

    // UserB and UserC cannot update any credentials (not owner, not metastore owner)
    for (CredentialsApi api : List.of(userBApi, userCApi)) {
      for (String credentialName : List.of(adminCredential, userACredential1, userACredential2)) {
        assertUpdateFailure(api, credentialName);
      }
    }

    // Test get and list operations

    // Admin can get and list all
    assertGetAndListPermissions(
        adminApi, List.of(adminCredential, userACredential1, userACredential2), List.of());

    // UserA can get and list its own credentials (owner)
    assertGetAndListPermissions(
        userAApi, List.of(userACredential1, userACredential2), List.of(adminCredential));

    // UserB can only get and list the credential it has CREATE_EXTERNAL_LOCATION on
    assertGetAndListPermissions(
        userBApi, List.of(userACredential1), List.of(adminCredential, userACredential2));

    // UserC cannot get or list any credentials (no permissions)
    assertGetAndListPermissions(
        userCApi, List.of(), List.of(adminCredential, userACredential1, userACredential2));

    // Test deletion operation

    // UserB and UserC cannot delete any credentials (not owner, not metastore owner)
    for (CredentialsApi api : List.of(userBApi, userCApi)) {
      for (String credentialName : List.of(adminCredential, userACredential1, userACredential2)) {
        assertDeleteFailure(api, credentialName);
      }
    }

    // UserA cannot delete admin's credential (not owner, not metastore owner)
    assertDeleteFailure(userAApi, adminCredential);

    // UserA can delete its own credentials (owner)
    assertDeleteSuccess(userAApi, userACredential2);

    // Admin can delete any credential (metastore owner)
    assertDeleteSuccess(adminApi, userACredential1);
    assertDeleteSuccess(adminApi, adminCredential);
  }

  private void assertCreateSuccess(CredentialsApi api, String name) throws ApiException {
    CredentialInfo created =
        api.createCredential(
            new CreateCredentialRequest()
                .name(name)
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)));
    credentialsToDelete.add(name);
    assertThat(created.getName()).isEqualTo(name);
    assertThat(created.getId()).isNotNull();
    assertThat(created.getPurpose()).isEqualTo(CredentialPurpose.STORAGE);
  }

  private void assertCreateFailure(CredentialsApi api, String name) {
    assertApiException(
        () ->
            api.createCredential(
                new CreateCredentialRequest()
                    .name(name)
                    .purpose(CredentialPurpose.STORAGE)
                    .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN))),
        ErrorCode.PERMISSION_DENIED,
        "Access denied.");
  }

  @SneakyThrows
  private void assertGetAndListPermissions(
      CredentialsApi api, List<String> allowedCredentialNames, List<String> deniedCredentialNames) {
    // Test get operation
    for (String name : allowedCredentialNames) {
      CredentialInfo retrieved = api.getCredential(name);
      assertThat(retrieved.getName()).isEqualTo(name);
      assertThat(retrieved.getId()).isNotNull().isNotEmpty();
    }
    for (String name : deniedCredentialNames) {
      assertApiException(
          () -> api.getCredential(name), ErrorCode.PERMISSION_DENIED, "Access denied");
    }
    // Test list operation
    ListCredentialsResponse response = api.listCredentials(100, null, null);
    assertThat(response.getNextPageToken()).isNull();
    response.getCredentials().forEach(cred -> assertThat(cred.getId()).isNotNull().isNotEmpty());
    assertThat(response.getCredentials().stream().map(CredentialInfo::getName))
        .containsExactlyInAnyOrderElementsOf(allowedCredentialNames);
  }

  private void assertUpdateSuccess(CredentialsApi api, String name) throws ApiException {
    updateCounter++;
    String newRoleArn = "arn:aws:iam::123456789012:role/updated-" + updateCounter;
    CredentialInfo updated =
        api.updateCredential(
            name,
            new UpdateCredentialRequest().awsIamRole(new AwsIamRoleRequest().roleArn(newRoleArn)));
    assertThat(updated.getAwsIamRole().getRoleArn()).isEqualTo(newRoleArn);
  }

  private void assertUpdateFailure(CredentialsApi api, String name) {
    assertApiException(
        () ->
            api.updateCredential(
                name,
                new UpdateCredentialRequest()
                    .awsIamRole(
                        new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/fail"))),
        ErrorCode.PERMISSION_DENIED,
        "Access denied");
  }

  private void assertDeleteSuccess(CredentialsApi api, String name) throws ApiException {
    api.deleteCredential(name, false);
    credentialsToDelete.remove(name);
    // Verify deletion by checking that get fails with 404
    assertApiException(() -> adminApi.getCredential(name), ErrorCode.NOT_FOUND, "not found");
  }

  private void assertDeleteFailure(CredentialsApi api, String name) {
    assertApiException(
        () -> api.deleteCredential(name, false), ErrorCode.PERMISSION_DENIED, "Access denied");
  }
}
