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
import io.unitycatalog.client.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.client.model.GcpServiceAccountKeyResponse;
import io.unitycatalog.client.model.S3AssumeRole;
import io.unitycatalog.client.model.S3CompatibleRequest;
import io.unitycatalog.client.model.S3CompatibleResponse;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
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
  private static final String NOT_AN_HTTP_URL =
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
                "Specify exactly one of aws_iam_role, s3_compatible and gcp_service_account_key."),
            new InvalidCreate(
                request(null),
                "Specify exactly one of aws_iam_role, s3_compatible and gcp_service_account_key."),
            new InvalidCreate(
                request(
                    keyPair()
                        .assumeRole(
                            new S3AssumeRole().roleArn(DUMMY_ROLE_ARN).stsRegion("us east 1"))),
                "Invalid s3_compatible.assume_role.sts_region: use letters, digits, '.', '_' and"
                    + " '-'."),
            new InvalidCreate(
                request(keyPair().accessKeyId(null)), "s3_compatible.access_key_id is required."),
            new InvalidCreate(
                request(keyPair().secretAccessKey(null)),
                "s3_compatible.secret_access_key is required."),
            new InvalidCreate(
                request(
                    keyPair()
                        .assumeRole(new S3AssumeRole().stsEndpoint("https://sts.example.com"))),
                "s3_compatible.assume_role.role_arn is required."),
            new InvalidCreate(
                request(keyPair().s3Endpoint(null)), "s3_compatible.s3_endpoint is required."),
            new InvalidCreate(
                request(keyPair().s3Endpoint("storage.example.com:9000")),
                "Invalid s3_compatible.s3_endpoint:" + NOT_AN_HTTP_URL),
            new InvalidCreate(
                request(
                    keyPair()
                        .assumeRole(
                            new S3AssumeRole()
                                .stsEndpoint("storage.example.com")
                                .roleArn(DUMMY_ROLE_ARN))),
                "Invalid s3_compatible.assume_role.sts_endpoint:" + NOT_AN_HTTP_URL),
            // java.net.URI finds no host in a name with an underscore, and neither would a client.
            new InvalidCreate(
                request(keyPair().s3Endpoint("http://minio_1:9000")),
                "Invalid s3_compatible.s3_endpoint:" + NOT_AN_HTTP_URL),
            new InvalidCreate(
                request(keyPair().s3Endpoint("http://storage.example.com:0")),
                "Invalid s3_compatible.s3_endpoint:" + NOT_AN_HTTP_URL),
            new InvalidCreate(
                request(keyPair().s3Endpoint("http://storage.example.com:65536")),
                "Invalid s3_compatible.s3_endpoint:" + NOT_AN_HTTP_URL),
            // The S3 and STS APIs are at the root of the host, named exactly.
            new InvalidCreate(
                request(keyPair().s3Endpoint("https://storage.example.com:9000/s3")),
                "Invalid s3_compatible.s3_endpoint: must be http[s]://host[:port], without a"
                    + " path."),
            new InvalidCreate(
                request(keyPair().s3Endpoint("https://storage.example.com:9000/")),
                "Invalid s3_compatible.s3_endpoint: must be http[s]://host[:port], without a"
                    + " path."),
            new InvalidCreate(
                request(keyPair().s3Endpoint("https://storage.example.com:")),
                "Invalid s3_compatible.s3_endpoint: must be http[s]://host[:port], without a"
                    + " path."),
            new InvalidCreate(
                request(
                    keyPair()
                        .assumeRole(
                            new S3AssumeRole()
                                .stsEndpoint("https://sts.example.com/sts/")
                                .roleArn(DUMMY_ROLE_ARN))),
                "Invalid s3_compatible.assume_role.sts_endpoint: must be http[s]://host[:port],"
                    + " without a path."),
            // A rejected value is left out of the message, since it may carry something secret.
            new InvalidCreate(
                request(keyPair().s3Endpoint("https://user:value@storage.example.com/%ZZ")),
                "Invalid s3_compatible.s3_endpoint: not a URL."),
            new InvalidCreate(
                request(keyPair().s3Endpoint("user:value@storage.example.com")),
                "Invalid s3_compatible.s3_endpoint:" + NOT_AN_HTTP_URL),
            // Endpoints are readable by everyone who can read the credential.
            new InvalidCreate(
                request(keyPair().s3Endpoint("https://user:value@storage.example.com")),
                "Invalid s3_compatible.s3_endpoint: must not contain user info, a query or a"
                    + " fragment."),
            new InvalidCreate(
                request(
                    keyPair()
                        .assumeRole(
                            new S3AssumeRole()
                                .stsEndpoint("https://sts.example.com/?token=value")
                                .roleArn(DUMMY_ROLE_ARN))),
                "Invalid s3_compatible.assume_role.sts_endpoint: must not contain user info, a"
                    + " query or a fragment."),
            new InvalidCreate(
                request(keyPair().s3Endpoint("https://storage.example.com/#value")),
                "Invalid s3_compatible.s3_endpoint: must not contain user info, a query or a"
                    + " fragment."));
    for (InvalidCreate c : invalidCreates) {
      assertApiException(
          () -> credentialOperations.createCredential(c.request()),
          ErrorCode.INVALID_ARGUMENT,
          c.error());
    }
    for (String endpoint :
        List.of("https://user:value@storage.example.com/%ZZ", "user:value@storage.example.com")) {
      assertThatThrownBy(
              () -> credentialOperations.createCredential(request(keyPair().s3Endpoint(endpoint))))
          .isInstanceOf(ApiException.class)
          .extracting(e -> ((ApiException) e).getResponseBody())
          .asString()
          .doesNotContain("user:value");
    }

    // Without assume_role, the key pair is the credential's only identity.
    CredentialInfo created = credentialOperations.createCredential(request(keyPair()));
    assertThat(created.getS3Compatible())
        .isEqualTo(new S3CompatibleResponse().accessKeyId("key-id-1").s3Endpoint(S3_ENDPOINT));
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
                    .s3Compatible(keyPair())
                    .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN))),
        ErrorCode.INVALID_ARGUMENT,
        "Specify only one of aws_iam_role and s3_compatible.");
    assertApiException(
        () ->
            credentialOperations.updateCredential(
                CREDENTIAL_NAME,
                new UpdateCredentialRequest().s3Compatible(keyPair().s3Endpoint("not-a-url"))),
        ErrorCode.INVALID_ARGUMENT,
        "Invalid s3_compatible.s3_endpoint:" + NOT_AN_HTTP_URL);
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(created);

    // An update replaces the whole s3_compatible, or switches the credential to another type.
    // sts_endpoint is returned as set: without one, AssumeRole goes to s3_endpoint at vend time.
    for (String stsEndpoint : Arrays.asList("http://sts:9000", null)) {
      S3AssumeRole requested = new S3AssumeRole().stsEndpoint(stsEndpoint).roleArn(S3_ROLE_ARN);
      CredentialInfo withAssumeRole =
          credentialOperations.updateCredential(
              CREDENTIAL_NAME,
              new UpdateCredentialRequest().s3Compatible(keyPair().assumeRole(requested)));
      assertThat(withAssumeRole.getS3Compatible())
          .isEqualTo(
              new S3CompatibleResponse()
                  .accessKeyId("key-id-1")
                  .s3Endpoint(S3_ENDPOINT)
                  .assumeRole(requested));
    }
    CredentialInfo onAws =
        credentialOperations.updateCredential(
            CREDENTIAL_NAME,
            new UpdateCredentialRequest()
                .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)));
    assertThat(onAws.getS3Compatible()).isNull();
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
    assertThat(created.getS3Compatible().getAssumeRole()).isEqualTo(assumeRole);
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(created);
    assertThat(credentialOperations.listCredentials(Optional.empty(), CredentialPurpose.STORAGE))
        .contains(created);

    String updatedArn = roleArn + "-2";
    CredentialInfo updated =
        credentialOperations.updateCredential(
            CREDENTIAL_NAME,
            new UpdateCredentialRequest()
                .s3Compatible(keyPair().assumeRole(assumeRole.roleArn(updatedArn))));
    assertThat(updated.getS3Compatible().getAssumeRole().getRoleArn()).isEqualTo(updatedArn);
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(updated);
    credentialOperations.deleteCredential(CREDENTIAL_NAME, Optional.empty());
  }

  @Test
  public void testGcpServiceAccountKeyCredential() throws ApiException {
    String privateKey = TestUtils.generatePkcs8PrivateKeyPem();
    String missing = "gcp_service_account_key.%s is required.";
    List<InvalidCreate> invalidCreates =
        List.of(
            new InvalidCreate(
                gcpRequest(privateKey).awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)),
                "Specify exactly one of aws_iam_role, s3_compatible and gcp_service_account_key."),
            new InvalidCreate(gcpRequest(privateKey, " "), String.format(missing, "email")),
            new InvalidCreate(
                gcpRequest(privateKey).gcpServiceAccountKey(gcpKey(privateKey).privateKeyId(null)),
                String.format(missing, "private_key_id")),
            new InvalidCreate(
                gcpRequest(privateKey).gcpServiceAccountKey(gcpKey(null)),
                String.format(missing, "private_key")),
            new InvalidCreate(
                gcpRequest("not-a-key"),
                "gcp_service_account_key.private_key is not a PEM-encoded PKCS#8 private key."),
            new InvalidCreate(
                gcpRequest(privateKey, "e".repeat(1025)),
                "gcp_service_account_key.email is longer than 1024 characters."),
            new InvalidCreate(
                gcpRequest(privateKey)
                    .gcpServiceAccountKey(gcpKey(privateKey).privateKeyId("k".repeat(1025))),
                "gcp_service_account_key.private_key_id is longer than 1024 characters."),
            new InvalidCreate(
                gcpRequest("p".repeat(4097)),
                "gcp_service_account_key.private_key is longer than 4096 characters."));
    for (InvalidCreate c : invalidCreates) {
      assertApiException(
          () -> credentialOperations.createCredential(c.request()),
          ErrorCode.INVALID_ARGUMENT,
          c.error());
    }
    // A rejected key is left out of the message.
    assertThatThrownBy(() -> credentialOperations.createCredential(gcpRequest("not-a-key")))
        .isInstanceOf(ApiException.class)
        .extracting(e -> ((ApiException) e).getResponseBody())
        .asString()
        .doesNotContain("not-a-key");

    // The private key is write-only: every read returns the email and key id only.
    CredentialInfo created = credentialOperations.createCredential(gcpRequest(privateKey));
    GcpServiceAccountKeyResponse expected =
        new GcpServiceAccountKeyResponse().email(GCP_EMAIL).privateKeyId(GCP_PRIVATE_KEY_ID);
    assertThat(created.getGcpServiceAccountKey()).isEqualTo(expected);
    assertThat(created.getAwsIamRole()).isNull();
    assertThat(credentialOperations.getCredential(CREDENTIAL_NAME)).isEqualTo(created);
    assertThat(credentialOperations.listCredentials(Optional.empty(), CredentialPurpose.STORAGE))
        .contains(created);

    // Its key cannot be changed through update, nor can it become another type, since its
    // locations are gs:// paths.
    assertApiException(
        () ->
            credentialOperations.updateCredential(
                CREDENTIAL_NAME,
                new UpdateCredentialRequest()
                    .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN))),
        ErrorCode.INVALID_ARGUMENT,
        "is a gcp_service_account_key credential, so aws_iam_role and s3_compatible cannot be"
            + " set on it.");
    assertThat(
            credentialOperations
                .updateCredential(CREDENTIAL_NAME, new UpdateCredentialRequest().comment(COMMENT))
                .getGcpServiceAccountKey())
        .isEqualTo(expected);

    // Its external locations must be gs:// paths.
    String gsUrl = "gs://unitycatalog-test";
    String notGs =
        "Storage credential 'uc_testcredential' is a gcp_service_account_key credential, so its"
            + " external locations must be gs:// paths: ";
    assertApiException(
        () ->
            externalLocationOperations.createExternalLocation(
                new CreateExternalLocation()
                    .name(EXTERNAL_LOCATION_NAME)
                    .url(URL)
                    .credentialName(CREDENTIAL_NAME)),
        ErrorCode.INVALID_ARGUMENT,
        notGs + URL);
    externalLocationOperations.createExternalLocation(
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .url(gsUrl)
            .credentialName(CREDENTIAL_NAME));
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(URL)),
        ErrorCode.INVALID_ARGUMENT,
        notGs + URL);
    assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME).getUrl())
        .isEqualTo(gsUrl);
    // An S3-compatible credential would strand the gs:// location, as it would an AWS one.
    assertApiException(
        () ->
            credentialOperations.updateCredential(
                CREDENTIAL_NAME, new UpdateCredentialRequest().s3Compatible(keyPair())),
        ErrorCode.INVALID_ARGUMENT,
        "is a gcp_service_account_key credential");
    externalLocationOperations.deleteExternalLocation(EXTERNAL_LOCATION_NAME, Optional.empty());
    credentialOperations.deleteCredential(CREDENTIAL_NAME, Optional.empty());
  }

  private static final String GCP_EMAIL = "uc-test@my-project.iam.gserviceaccount.com";
  private static final String GCP_PRIVATE_KEY_ID = "0123456789abcdef";

  private static GcpServiceAccountKeyRequest gcpKey(String privateKey) {
    return new GcpServiceAccountKeyRequest()
        .email(GCP_EMAIL)
        .privateKeyId(GCP_PRIVATE_KEY_ID)
        .privateKey(privateKey);
  }

  private static CreateCredentialRequest gcpRequest(String privateKey) {
    return new CreateCredentialRequest()
        .name(CREDENTIAL_NAME)
        .purpose(CredentialPurpose.STORAGE)
        .gcpServiceAccountKey(gcpKey(privateKey));
  }

  private static CreateCredentialRequest gcpRequest(String privateKey, String email) {
    return gcpRequest(privateKey).gcpServiceAccountKey(gcpKey(privateKey).email(email));
  }

  private record InvalidCreate(CreateCredentialRequest request, String error) {}

  private static final String S3_ENDPOINT = "http://s3:9000";
  private static final String S3_ROLE_ARN = "arn:aws:iam::1:role/r";

  private static S3CompatibleRequest keyPair() {
    return new S3CompatibleRequest()
        .accessKeyId("key-id-1")
        .secretAccessKey("key-value-1")
        .s3Endpoint(S3_ENDPOINT);
  }

  private static CreateCredentialRequest request(S3CompatibleRequest s3Compatible) {
    return new CreateCredentialRequest()
        .name(CREDENTIAL_NAME)
        .purpose(CredentialPurpose.STORAGE)
        .s3Compatible(s3Compatible);
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
