package io.unitycatalog.server.base.credential;

import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.COMMENT2;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.exception.ErrorCode;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseCredentialCRUDTest extends BaseCRUDTest {
  private static final String CREDENTIAL_NAME = "uc_testcredential";
  private static final String NEW_CREDENTIAL_NAME = CREDENTIAL_NAME + "_new";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  private static final String NEW_ROLE_ARN = "arn:aws:iam::987654321098:role/new-role-name";
  private static final String EXTERNAL_LOCATION_NAME = "uc_testexternallocation";
  private static final String URL = "s3://unitycatalog-test";
  protected CredentialOperations credentialOperations;
  protected ExternalLocationOperations externalLocationOperations;

  protected abstract CredentialOperations createCredentialOperations(ServerConfig config);

  protected abstract ExternalLocationOperations createExternalLocationOperations(
      ServerConfig config);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    credentialOperations = createCredentialOperations(serverConfig);
    externalLocationOperations = createExternalLocationOperations(serverConfig);
  }

  @Test
  public void testStorageCredentialCRUD() throws ApiException {
    // Create a storage credential
    CreateCredentialRequest CreateCredentialRequest =
        new CreateCredentialRequest()
            .name(CREDENTIAL_NAME)
            .comment(COMMENT)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));

    CredentialInfo CredentialInfo = credentialOperations.createCredential(CreateCredentialRequest);
    assertThat(CredentialInfo.getName()).isEqualTo(CREDENTIAL_NAME);
    assertThat(CredentialInfo.getComment()).isEqualTo(COMMENT);
    assertThat(CredentialInfo.getPurpose()).isEqualTo(CredentialPurpose.STORAGE);

    // List storage credentials
    assertThat(credentialOperations.listCredentials(Optional.empty(), CredentialPurpose.STORAGE))
        .contains(CredentialInfo);

    // Get storage credential
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(CredentialInfo);

    // Update storage credential
    UpdateCredentialRequest UpdateCredentialRequest =
        new UpdateCredentialRequest()
            .newName(NEW_CREDENTIAL_NAME)
            .comment(COMMENT2)
            .awsIamRole(new AwsIamRoleRequest().roleArn(NEW_ROLE_ARN));

    CredentialInfo updatedCredentialInfo =
        credentialOperations.updateCredential(CREDENTIAL_NAME, UpdateCredentialRequest);
    assertThat(updatedCredentialInfo.getName()).isEqualTo(NEW_CREDENTIAL_NAME);
    assertThat(updatedCredentialInfo.getComment()).isEqualTo(COMMENT2);
    assertThat(updatedCredentialInfo.getAwsIamRole().getRoleArn()).isEqualTo(NEW_ROLE_ARN);

    // Delete storage credential
    credentialOperations.deleteCredential(NEW_CREDENTIAL_NAME, Optional.empty());
    assertThatThrownBy(() -> credentialOperations.getCredential(NEW_CREDENTIAL_NAME))
        .isInstanceOf(ApiException.class);
  }

  @Test
  public void testCredentialDeletion() throws ApiException {
    // Test 1: Credential with external location - delete without force should fail
    String credentialWithExternalLocationName = CREDENTIAL_NAME + "_with_el";
    CreateCredentialRequest createCredentialRequest =
        new CreateCredentialRequest()
            .name(credentialWithExternalLocationName)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));
    credentialOperations.createCredential(createCredentialRequest);

    // Create an external location that uses this credential
    CreateExternalLocation createExternalLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .url(URL)
            .credentialName(credentialWithExternalLocationName);
    externalLocationOperations.createExternalLocation(createExternalLocation);

    // Try to delete credential without force - should fail
    assertApiException(
        () ->
            credentialOperations.deleteCredential(
                credentialWithExternalLocationName, Optional.empty()),
        ErrorCode.INVALID_ARGUMENT,
        "Credential still used by external location");

    // Delete with force should succeed
    credentialOperations.deleteCredential(credentialWithExternalLocationName, Optional.of(true));
    // Clean up the external location
    externalLocationOperations.deleteExternalLocation(EXTERNAL_LOCATION_NAME, Optional.of(true));

    // Test 2: Credential without external location - delete without force should succeed
    String credentialWithoutExternalLocationName = CREDENTIAL_NAME + "_without_el";
    createCredentialRequest.setName(credentialWithoutExternalLocationName);
    credentialOperations.createCredential(createCredentialRequest);

    // Delete without force should succeed
    credentialOperations.deleteCredential(credentialWithoutExternalLocationName, Optional.empty());
  }
}
