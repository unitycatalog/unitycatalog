package io.unitycatalog.server.base.credential;

import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.COMMENT2;
import static io.unitycatalog.server.utils.TestUtils.TEST_AWS_MASTER_ROLE_ARN;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.AzureServicePrincipalRequest;
import io.unitycatalog.client.model.AzureServicePrincipalResponse;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.client.model.GcpServiceAccountKeyResponse;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseCredentialCRUDTest extends BaseCRUDTest {
  private static final String CREDENTIAL_NAME = "uc_testcredential";
  private static final String NEW_CREDENTIAL_NAME = CREDENTIAL_NAME + "_new";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  private static final String NEW_ROLE_ARN = "arn:aws:iam::987654321098:role/new-role-name";
  private static final String EXTERNAL_LOCATION_NAME = "uc_testexternallocation";
  private static final String URL = "s3://unitycatalog-test";
  protected static final String DIRECTORY_ID = "11111111-1111-1111-1111-111111111111";
  protected static final String APPLICATION_ID = "22222222-2222-2222-2222-222222222222";
  private static final String NEW_APPLICATION_ID = "33333333-3333-3333-3333-333333333333";
  protected static final String SERVICE_ACCOUNT_EMAIL = "uc@my-project.iam.gserviceaccount.com";
  protected static final String PRIVATE_KEY_ID = "0123456789abcdef0123456789abcdef01234567";
  private static final String NEW_PRIVATE_KEY_ID = "fedcba9876543210fedcba9876543210fedcba98";
  protected CredentialOperations credentialOperations;
  protected ExternalLocationOperations externalLocationOperations;

  protected abstract CredentialOperations createCredentialOperations(ServerConfig config);

  protected abstract ExternalLocationOperations createExternalLocationOperations(
      ServerConfig config);

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(
        ServerProperties.Property.AWS_MASTER_ROLE_ARN.getKey(), TEST_AWS_MASTER_ROLE_ARN);
  }

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
    CreateCredentialRequest createCredentialRequest =
        new CreateCredentialRequest()
            .name(CREDENTIAL_NAME)
            .comment(COMMENT)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));

    CredentialInfo credentialInfo = credentialOperations.createCredential(createCredentialRequest);
    assertThat(credentialInfo.getName()).isEqualTo(CREDENTIAL_NAME);
    assertThat(credentialInfo.getComment()).isEqualTo(COMMENT);
    assertThat(credentialInfo.getPurpose()).isEqualTo(CredentialPurpose.STORAGE);
    assertThat(credentialInfo.getAwsIamRole().getRoleArn()).isEqualTo(DUMMY_ROLE_ARN);
    assertThat(credentialInfo.getAwsIamRole().getExternalId()).isNotEmpty();
    assertDoesNotThrow(() -> UUID.fromString(credentialInfo.getAwsIamRole().getExternalId()));
    assertThat(credentialInfo.getAwsIamRole().getUnityCatalogIamArn())
        .isEqualTo(TEST_AWS_MASTER_ROLE_ARN);

    // List storage credentials
    assertThat(credentialOperations.listCredentials(Optional.empty(), CredentialPurpose.STORAGE))
        .contains(credentialInfo);

    // Get storage credential
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(credentialInfo);

    // Update storage credential
    UpdateCredentialRequest updateCredentialRequest =
        new UpdateCredentialRequest()
            .newName(NEW_CREDENTIAL_NAME)
            .comment(COMMENT2)
            .awsIamRole(new AwsIamRoleRequest().roleArn(NEW_ROLE_ARN));

    CredentialInfo updatedCredentialInfo =
        credentialOperations.updateCredential(CREDENTIAL_NAME, updateCredentialRequest);
    assertThat(updatedCredentialInfo.getName()).isEqualTo(NEW_CREDENTIAL_NAME);
    assertThat(updatedCredentialInfo.getComment()).isEqualTo(COMMENT2);
    assertThat(updatedCredentialInfo.getAwsIamRole().getRoleArn()).isEqualTo(NEW_ROLE_ARN);
    assertThat(updatedCredentialInfo.getAwsIamRole().getExternalId()).isNotEmpty();
    assertDoesNotThrow(
        () -> UUID.fromString(updatedCredentialInfo.getAwsIamRole().getExternalId()));
    assertThat(updatedCredentialInfo.getAwsIamRole().getExternalId())
        .isNotEqualTo(credentialInfo.getAwsIamRole().getExternalId());
    assertThat(updatedCredentialInfo.getAwsIamRole().getUnityCatalogIamArn())
        .isEqualTo(TEST_AWS_MASTER_ROLE_ARN);

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
    assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME))
        .returns(URL, ExternalLocationInfo::getUrl)
        .returns(null, ExternalLocationInfo::getCredentialName);
    // Clean up the external location
    externalLocationOperations.deleteExternalLocation(EXTERNAL_LOCATION_NAME, Optional.of(true));

    // Test 2: Credential without external location - delete without force should succeed
    String credentialWithoutExternalLocationName = CREDENTIAL_NAME + "_without_el";
    createCredentialRequest.setName(credentialWithoutExternalLocationName);
    credentialOperations.createCredential(createCredentialRequest);

    // Delete without force should succeed
    credentialOperations.deleteCredential(credentialWithoutExternalLocationName, Optional.empty());
  }

  protected static AzureServicePrincipalRequest azureServicePrincipal(String clientSecret) {
    return new AzureServicePrincipalRequest()
        .directoryId(DIRECTORY_ID)
        .applicationId(APPLICATION_ID)
        .clientSecret(clientSecret);
  }

  protected static GcpServiceAccountKeyRequest gcpServiceAccountKey(String privateKey) {
    return new GcpServiceAccountKeyRequest()
        .email(SERVICE_ACCOUNT_EMAIL)
        .privateKeyId(PRIVATE_KEY_ID)
        .privateKey(privateKey);
  }

  @Test
  public void testAzureServicePrincipalCredentialCRUD() throws ApiException {
    CredentialInfo credentialInfo =
        credentialOperations.createCredential(
            new CreateCredentialRequest()
                .name(CREDENTIAL_NAME)
                .purpose(CredentialPurpose.STORAGE)
                .azureServicePrincipal(azureServicePrincipal("secret")));
    // The response carries the identity but not the secret; the response model has no field for it.
    assertThat(credentialInfo.getAzureServicePrincipal())
        .isEqualTo(
            new AzureServicePrincipalResponse()
                .directoryId(DIRECTORY_ID)
                .applicationId(APPLICATION_ID));
    assertThat(credentialInfo.getAwsIamRole()).isNull();
    assertThat(credentialInfo.getGcpServiceAccountKey()).isNull();
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(credentialInfo);
    assertThat(credentialOperations.listCredentials(Optional.empty(), CredentialPurpose.STORAGE))
        .contains(credentialInfo);

    // Rotating the secret, or moving to another application, goes through update.
    CredentialInfo updatedCredentialInfo =
        credentialOperations.updateCredential(
            CREDENTIAL_NAME,
            new UpdateCredentialRequest()
                .comment(COMMENT2)
                .azureServicePrincipal(
                    azureServicePrincipal("rotated").applicationId(NEW_APPLICATION_ID)));
    assertThat(updatedCredentialInfo.getComment()).isEqualTo(COMMENT2);
    assertThat(updatedCredentialInfo.getAzureServicePrincipal())
        .isEqualTo(
            new AzureServicePrincipalResponse()
                .directoryId(DIRECTORY_ID)
                .applicationId(NEW_APPLICATION_ID));
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME))
        .isEqualTo(updatedCredentialInfo);

    credentialOperations.deleteCredential(CREDENTIAL_NAME, Optional.empty());
    assertApiException(
        () -> credentialOperations.getCredential(CREDENTIAL_NAME),
        ErrorCode.NOT_FOUND,
        "Storage credential not found");
  }

  @Test
  public void testGcpServiceAccountKeyCredentialCRUD() throws ApiException {
    CredentialInfo credentialInfo =
        credentialOperations.createCredential(
            new CreateCredentialRequest()
                .name(CREDENTIAL_NAME)
                .purpose(CredentialPurpose.STORAGE)
                .gcpServiceAccountKey(gcpServiceAccountKey("pkcs8-private-key")));
    assertThat(credentialInfo.getGcpServiceAccountKey())
        .isEqualTo(
            new GcpServiceAccountKeyResponse()
                .email(SERVICE_ACCOUNT_EMAIL)
                .privateKeyId(PRIVATE_KEY_ID));
    assertThat(credentialInfo.getAwsIamRole()).isNull();
    assertThat(credentialInfo.getAzureServicePrincipal()).isNull();
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(credentialInfo);
    assertThat(credentialOperations.listCredentials(Optional.empty(), CredentialPurpose.STORAGE))
        .contains(credentialInfo);

    CredentialInfo updatedCredentialInfo =
        credentialOperations.updateCredential(
            CREDENTIAL_NAME,
            new UpdateCredentialRequest()
                .gcpServiceAccountKey(
                    gcpServiceAccountKey("rotated-pkcs8-private-key")
                        .privateKeyId(NEW_PRIVATE_KEY_ID)));
    assertThat(updatedCredentialInfo.getGcpServiceAccountKey())
        .isEqualTo(
            new GcpServiceAccountKeyResponse()
                .email(SERVICE_ACCOUNT_EMAIL)
                .privateKeyId(NEW_PRIVATE_KEY_ID));
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME))
        .isEqualTo(updatedCredentialInfo);

    credentialOperations.deleteCredential(CREDENTIAL_NAME, Optional.empty());
    assertApiException(
        () -> credentialOperations.getCredential(CREDENTIAL_NAME),
        ErrorCode.NOT_FOUND,
        "Storage credential not found");
  }

  @Test
  public void testCredentialRequiresExactlyOneCloudIdentity() {
    String exactlyOne =
        "Exactly one of aws_iam_role, azure_service_principal or gcp_service_account_key must be"
            + " set";
    CreateCredentialRequest request =
        new CreateCredentialRequest().name(CREDENTIAL_NAME).purpose(CredentialPurpose.STORAGE);
    assertApiException(
        () -> credentialOperations.createCredential(request),
        ErrorCode.INVALID_ARGUMENT,
        exactlyOne);
    assertApiException(
        () ->
            credentialOperations.createCredential(
                request
                    .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN))
                    .azureServicePrincipal(azureServicePrincipal("secret"))),
        ErrorCode.INVALID_ARGUMENT,
        exactlyOne);
    // Required fields are checked at create time, not when the credential is first vended.
    assertApiException(
        () ->
            credentialOperations.createCredential(
                request.awsIamRole(null).azureServicePrincipal(azureServicePrincipal(""))),
        ErrorCode.INVALID_ARGUMENT,
        "azure_service_principal.client_secret must not be empty");
    assertApiException(
        () ->
            credentialOperations.createCredential(
                request
                    .azureServicePrincipal(null)
                    .gcpServiceAccountKey(gcpServiceAccountKey("key").privateKeyId(" "))),
        ErrorCode.INVALID_ARGUMENT,
        "gcp_service_account_key.private_key_id must not be empty");
    assertThatThrownBy(() -> credentialOperations.getCredential(CREDENTIAL_NAME))
        .isInstanceOf(ApiException.class);
  }

  @Test
  public void testCredentialCloudCannotChange() throws ApiException {
    CredentialInfo credentialInfo =
        credentialOperations.createCredential(
            new CreateCredentialRequest()
                .name(CREDENTIAL_NAME)
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)));

    // External locations bind the credential to URLs of its cloud, so the cloud cannot change.
    assertApiException(
        () ->
            credentialOperations.updateCredential(
                CREDENTIAL_NAME,
                new UpdateCredentialRequest().azureServicePrincipal(azureServicePrincipal("s"))),
        ErrorCode.INVALID_ARGUMENT,
        "Storage credential '"
            + CREDENTIAL_NAME
            + "' holds aws_iam_role and cannot be changed to azure_service_principal");
    assertApiException(
        () ->
            credentialOperations.updateCredential(
                CREDENTIAL_NAME,
                new UpdateCredentialRequest()
                    .awsIamRole(new AwsIamRoleRequest().roleArn(NEW_ROLE_ARN))
                    .gcpServiceAccountKey(gcpServiceAccountKey("key"))),
        ErrorCode.INVALID_ARGUMENT,
        "At most one of aws_iam_role, azure_service_principal or gcp_service_account_key may be"
            + " set");
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(credentialInfo);

    credentialOperations.deleteCredential(CREDENTIAL_NAME, Optional.empty());
  }
}
