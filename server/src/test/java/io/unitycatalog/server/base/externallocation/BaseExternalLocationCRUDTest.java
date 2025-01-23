package io.unitycatalog.server.base.externallocation;

import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.COMMENT2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.storagecredential.StorageCredentialOperations;
import io.unitycatalog.server.exception.ErrorCode;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseExternalLocationCRUDTest extends BaseCRUDTest {
  private static final String EXTERNAL_LOCATION_NAME = "uc_testexternallocation";
  private static final String NEW_EXTERNAL_LOCATION_NAME = EXTERNAL_LOCATION_NAME + "_new";
  private static final String URL = "s3://unitycatalog-test";
  private static final String NEW_URL = "s3://unitycatalog-test-new";
  private static final String CREDENTIAL_NAME = "uc_testcredential";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  protected ExternalLocationOperations externalLocationOperations;

  protected abstract ExternalLocationOperations createExternalLocationOperations(
      ServerConfig config);

  protected StorageCredentialOperations storageCredentialOperations;

  protected abstract StorageCredentialOperations createStorageCredentialOperations(
      ServerConfig config);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    externalLocationOperations = createExternalLocationOperations(serverConfig);
    storageCredentialOperations = createStorageCredentialOperations(serverConfig);
  }

  @Test
  public void testExternalLocationCRUD() throws ApiException {
    // Create an external location
    CreateExternalLocation createExternalLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .comment(COMMENT)
            .url(URL)
            .credentialName(CREDENTIAL_NAME);
    // Fails as the credential does not exist
    assertThatThrownBy(
            () -> externalLocationOperations.createExternalLocation(createExternalLocation))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", ErrorCode.NOT_FOUND.getHttpStatus().code())
        .hasMessageContaining("Storage credential not found: " + CREDENTIAL_NAME);

    assertThat(externalLocationOperations.listExternalLocations(Optional.empty()))
        .noneMatch(
            externalLocationInfo -> externalLocationInfo.getName().equals(EXTERNAL_LOCATION_NAME));

    CreateStorageCredential createStorageCredential =
        new CreateStorageCredential()
            .name(CREDENTIAL_NAME)
            .comment(COMMENT)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));
    StorageCredentialInfo storageCredentialInfo =
        storageCredentialOperations.createStorageCredential(createStorageCredential);

    ExternalLocationInfo externalLocationInfo =
        externalLocationOperations.createExternalLocation(createExternalLocation);
    assertThat(externalLocationInfo.getName()).isEqualTo(EXTERNAL_LOCATION_NAME);
    assertThat(externalLocationInfo.getComment()).isEqualTo(COMMENT);
    assertThat(externalLocationInfo.getUrl()).isEqualTo(URL);
    assertThat(externalLocationInfo.getCredentialId()).isEqualTo(storageCredentialInfo.getId());

    // List external locations
    assertThat(externalLocationOperations.listExternalLocations(Optional.empty()))
        .contains(externalLocationInfo);

    // Get external location
    assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME))
        .isEqualTo(externalLocationInfo);

    // Update external location
    UpdateExternalLocation updateExternalLocation =
        new UpdateExternalLocation()
            .newName(NEW_EXTERNAL_LOCATION_NAME)
            .comment(COMMENT2)
            .url(NEW_URL);
    ExternalLocationInfo updatedExternalLocationInfo =
        externalLocationOperations.updateExternalLocation(
            EXTERNAL_LOCATION_NAME, updateExternalLocation);
    assertThat(updatedExternalLocationInfo.getName()).isEqualTo(NEW_EXTERNAL_LOCATION_NAME);
    assertThat(updatedExternalLocationInfo.getComment()).isEqualTo(COMMENT2);
    assertThat(updatedExternalLocationInfo.getUrl()).isEqualTo(NEW_URL);
    assertThat(updatedExternalLocationInfo.getCredentialId())
        .isEqualTo(storageCredentialInfo.getId());

    // Delete external location
    externalLocationOperations.deleteExternalLocation(NEW_EXTERNAL_LOCATION_NAME);
    assertThatThrownBy(
            () -> externalLocationOperations.getExternalLocation(NEW_EXTERNAL_LOCATION_NAME))
        .isInstanceOf(ApiException.class);
  }
}
