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
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.S3AssumeRole;
import io.unitycatalog.client.model.S3CompatibleAccessKeyRequest;
import io.unitycatalog.client.model.S3CompatibleAccessKeyResponse;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseCredentialCRUDTest extends BaseCRUDTest {
  private static final String CREDENTIAL_NAME = "uc_testcredential";
  private static final String NEW_CREDENTIAL_NAME = CREDENTIAL_NAME + "_new";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  private static final String NEW_ROLE_ARN = "arn:aws:iam::987654321098:role/new-role-name";
  public static final String NOT_AN_HTTP_URL =
      " must be an http or https URL with a host (a host name holds only letters, digits, '-'"
          + " and '.') and, if any, a port from 1 to 65535.";
  private static final String EXTERNAL_LOCATION_NAME = "uc_testexternallocation";
  private static final String URL = "s3://unitycatalog-test";
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
    enableS3CompatibleStorage();
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
  public void testS3CompatibleCredential() throws ApiException {
    List<InvalidCreate> invalidCreates =
        List.of(
            new InvalidCreate(
                request(keyPair()).awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)),
                "Specify only one of aws_iam_role and s3_compatible_access_key."),
            new InvalidCreate(
                request(null), "Specify exactly one of aws_iam_role and s3_compatible_access_key."),
            new InvalidCreate(
                request(
                    keyPair()
                        .assumeRole(
                            new S3AssumeRole().roleArn(DUMMY_ROLE_ARN).stsRegion("us east 1"))),
                "Invalid s3_compatible_access_key.assume_role.sts_region: use letters, digits, '.',"
                    + " '_' and '-'."),
            new InvalidCreate(
                request(keyPair().accessKeyId(null)),
                "s3_compatible_access_key.access_key_id is required."),
            new InvalidCreate(
                request(keyPair().secretAccessKey(null)),
                "s3_compatible_access_key.secret_access_key is required."),
            new InvalidCreate(
                request(
                    keyPair()
                        .assumeRole(new S3AssumeRole().stsEndpoint("https://sts.example.com"))),
                "s3_compatible_access_key.assume_role.role_arn is required."),
            // The endpoint forms of the location's endpoint (BaseExternalLocationCRUDTest) apply.
            new InvalidCreate(
                request(
                    keyPair()
                        .assumeRole(
                            new S3AssumeRole()
                                .stsEndpoint("storage.example.com")
                                .roleArn(DUMMY_ROLE_ARN))),
                "Invalid s3_compatible_access_key.assume_role.sts_endpoint:" + NOT_AN_HTTP_URL),
            new InvalidCreate(
                request(
                    keyPair()
                        .assumeRole(
                            new S3AssumeRole()
                                .stsEndpoint("https://sts.example.com/sts/")
                                .roleArn(DUMMY_ROLE_ARN))),
                "Invalid s3_compatible_access_key.assume_role.sts_endpoint: must be"
                    + " http[s]://host[:port], without a path."),
            new InvalidCreate(
                request(
                    keyPair()
                        .assumeRole(
                            new S3AssumeRole()
                                .stsEndpoint("https://sts.example.com/?token=value")
                                .roleArn(DUMMY_ROLE_ARN))),
                "Invalid s3_compatible_access_key.assume_role.sts_endpoint: must not contain user"
                    + " info, a query or a fragment."));
    for (InvalidCreate c : invalidCreates) {
      assertApiException(
          () -> credentialOperations.createCredential(c.request()),
          ErrorCode.INVALID_ARGUMENT,
          c.error());
    }

    // Without assume_role, the key pair is the credential's only identity.
    CredentialInfo created = credentialOperations.createCredential(request(keyPair()));
    assertThat(created.getS3CompatibleAccessKey())
        .isEqualTo(new S3CompatibleAccessKeyResponse().accessKeyId("key-id-1"));
    assertThat(created.getAwsIamRole()).isNull();
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(created);
    // A rejected update keeps the stored credential. Both types are refused before the new name is
    // looked up, so the name clash does not decide the error.
    assertApiException(
        () ->
            credentialOperations.updateCredential(
                CREDENTIAL_NAME,
                new UpdateCredentialRequest()
                    .newName(CREDENTIAL_NAME)
                    .s3CompatibleAccessKey(keyPair())
                    .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN))),
        ErrorCode.INVALID_ARGUMENT,
        "Specify only one of aws_iam_role and s3_compatible_access_key.");
    assertApiException(
        () ->
            credentialOperations.updateCredential(
                CREDENTIAL_NAME,
                new UpdateCredentialRequest()
                    .s3CompatibleAccessKey(keyPair().secretAccessKey(null))),
        ErrorCode.INVALID_ARGUMENT,
        "s3_compatible_access_key.secret_access_key is required.");
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(created);

    // An update replaces the whole s3_compatible_access_key, or switches a credential that no
    // external location uses to another type. sts_endpoint is returned as set: without one,
    // AssumeRole goes to the endpoint of the path's external location at vend time.
    for (String stsEndpoint : Arrays.asList("http://sts:9000", null)) {
      S3AssumeRole requested = new S3AssumeRole().stsEndpoint(stsEndpoint).roleArn(S3_ROLE_ARN);
      CredentialInfo withAssumeRole =
          credentialOperations.updateCredential(
              CREDENTIAL_NAME,
              new UpdateCredentialRequest().s3CompatibleAccessKey(keyPair().assumeRole(requested)));
      assertThat(withAssumeRole.getS3CompatibleAccessKey())
          .isEqualTo(
              new S3CompatibleAccessKeyResponse().accessKeyId("key-id-1").assumeRole(requested));
    }
    CredentialInfo onAws =
        credentialOperations.updateCredential(
            CREDENTIAL_NAME,
            new UpdateCredentialRequest()
                .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)));
    assertThat(onAws.getS3CompatibleAccessKey()).isNull();
    assertThat(onAws.getAwsIamRole().getRoleArn()).isEqualTo(DUMMY_ROLE_ARN);
    credentialOperations.deleteCredential(CREDENTIAL_NAME, Optional.empty());
  }

  @Test
  public void testLongS3CompatibleCredential() throws ApiException {
    // Longer than MySQL's tinytext (255 bytes) once stored, with non-ASCII text, on every database.
    String roleArn = "arn:aws:iam::123456789012:role/réle-" + "r".repeat(1000);
    S3AssumeRole assumeRole = new S3AssumeRole().stsEndpoint(S3_ENDPOINT).roleArn(roleArn);
    CredentialInfo created =
        credentialOperations.createCredential(request(keyPair().assumeRole(assumeRole)));
    assertThat(created.getS3CompatibleAccessKey().getAssumeRole()).isEqualTo(assumeRole);
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(created);
    assertThat(credentialOperations.listCredentials(Optional.empty(), CredentialPurpose.STORAGE))
        .contains(created);

    String updatedArn = roleArn + "-2";
    CredentialInfo updated =
        credentialOperations.updateCredential(
            CREDENTIAL_NAME,
            new UpdateCredentialRequest()
                .s3CompatibleAccessKey(keyPair().assumeRole(assumeRole.roleArn(updatedArn))));
    assertThat(updated.getS3CompatibleAccessKey().getAssumeRole().getRoleArn())
        .isEqualTo(updatedArn);
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(updated);
    credentialOperations.deleteCredential(CREDENTIAL_NAME, Optional.empty());
  }

  private record InvalidCreate(CreateCredentialRequest request, String error) {}

  private static final String S3_ENDPOINT = "http://s3:9000";
  private static final String S3_ROLE_ARN = "arn:aws:iam::1:role/r";

  private static S3CompatibleAccessKeyRequest keyPair() {
    return new S3CompatibleAccessKeyRequest()
        .accessKeyId("key-id-1")
        .secretAccessKey("key-value-1");
  }

  private static CreateCredentialRequest request(S3CompatibleAccessKeyRequest accessKey) {
    return new CreateCredentialRequest()
        .name(CREDENTIAL_NAME)
        .purpose(CredentialPurpose.STORAGE)
        .s3CompatibleAccessKey(accessKey);
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
}
