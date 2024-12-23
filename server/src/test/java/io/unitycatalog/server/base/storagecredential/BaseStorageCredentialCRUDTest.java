package io.unitycatalog.server.base.storagecredential;

import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.COMMENT2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseStorageCredentialCRUDTest extends BaseCRUDTest {
  private static final String CREDENTIAL_NAME = "uc_testcredential";
  private static final String NEW_CREDENTIAL_NAME = CREDENTIAL_NAME + "_new";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  protected StorageCredentialOperations storageCredentialOperations;

  protected abstract StorageCredentialOperations createStorageCredentialOperations(
      ServerConfig config);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    storageCredentialOperations = createStorageCredentialOperations(serverConfig);
  }

  @Test
  public void testStorageCredentialCRUD() throws ApiException {
    // Create a storage credential
    CreateStorageCredential createStorageCredential =
        new CreateStorageCredential()
            .name(CREDENTIAL_NAME)
            .comment(COMMENT)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));

    StorageCredentialInfo storageCredentialInfo =
        storageCredentialOperations.createStorageCredential(createStorageCredential);
    assertThat(storageCredentialInfo.getName()).isEqualTo(CREDENTIAL_NAME);
    assertThat(storageCredentialInfo.getComment()).isEqualTo(COMMENT);

    // List storage credentials
    assertThat(storageCredentialOperations.listStorageCredentials(Optional.empty()))
        .contains(storageCredentialInfo);

    // Get storage credential
    assertThat(storageCredentialOperations.getStorageCredential(CREDENTIAL_NAME))
        .isEqualTo(storageCredentialInfo);

    // Update storage credential
    UpdateStorageCredential updateStorageCredential =
        new UpdateStorageCredential().newName(NEW_CREDENTIAL_NAME).comment(COMMENT2);
    StorageCredentialInfo updatedStorageCredentialInfo =
        storageCredentialOperations.updateStorageCredential(
            CREDENTIAL_NAME, updateStorageCredential);
    assertThat(updatedStorageCredentialInfo.getName()).isEqualTo(NEW_CREDENTIAL_NAME);
    assertThat(updatedStorageCredentialInfo.getComment()).isEqualTo(COMMENT2);

    // Delete storage credential
    storageCredentialOperations.deleteStorageCredential(NEW_CREDENTIAL_NAME);
    assertThatThrownBy(() -> storageCredentialOperations.getStorageCredential(NEW_CREDENTIAL_NAME))
        .isInstanceOf(ApiException.class);
  }
}
