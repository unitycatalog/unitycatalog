package io.unitycatalog.server.base.credential;

import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.COMMENT2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseCredentialCRUDTest extends BaseCRUDTest {
  private static final String CREDENTIAL_NAME = "uc_testcredential";
  private static final String NEW_CREDENTIAL_NAME = CREDENTIAL_NAME + "_new";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  private static final String NEW_ROLE_ARN = "arn:aws:iam::987654321098:role/new-role-name";
  protected CredentialOperations credentialOperations;

  protected abstract CredentialOperations createCredentialOperations(ServerConfig config);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    credentialOperations = createCredentialOperations(serverConfig);
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
    credentialOperations.deleteCredential(NEW_CREDENTIAL_NAME);
    assertThatThrownBy(() -> credentialOperations.getCredential(NEW_CREDENTIAL_NAME))
        .isInstanceOf(ApiException.class);
  }
}
