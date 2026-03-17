package io.unitycatalog.server.service.credential;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcsStorageConfig;
import io.unitycatalog.server.service.credential.gcp.StaticTestingCredentialGenerator;
import io.unitycatalog.server.service.credential.gcp.TestingCredentialGenerator;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.StsException;

@ExtendWith(MockitoExtension.class)
public class CloudCredentialVendorTest {
  @Mock ServerProperties serverProperties;
  @Mock ExternalLocationUtils externalLocationUtils;
  CloudCredentialVendor credentialsOperations;

  @BeforeEach
  public void setUp() {
    // Assumes no external location (or credential)
    doReturn(Optional.empty())
        .when(externalLocationUtils)
        .getExternalLocationCredentialDaoForPath(any());
  }

  private TemporaryCredentials vendCredential(
      String path, Set<CredentialContext.Privilege> privileges) {
    StorageCredentialVendor storageCredentialVendor =
        new StorageCredentialVendor(credentialsOperations, externalLocationUtils);
    return storageCredentialVendor.vendCredential(NormalizedURL.from(path), privileges);
  }

  @Test
  public void testGenerateS3TemporaryCredentials() {
    final String ACCESS_KEY = "accessKey";
    final String SECRET_KEY = "secretKey";
    final String SESSION_TOKEN = "sessionToken";
    final String S3_REGION = "us-west-2";
    final String ROLE_ARN = "roleArn";
    // Test session key is available
    when(serverProperties.getS3Configurations())
        .thenReturn(
            Map.of(
                NormalizedURL.from("s3://storageBase"),
                S3StorageConfig.builder()
                    .accessKey(ACCESS_KEY)
                    .secretKey(SECRET_KEY)
                    .sessionToken(SESSION_TOKEN)
                    .build()));
    AwsCredentialVendor awsCredentialVendor = new AwsCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(awsCredentialVendor, null, null);
    TemporaryCredentials s3TemporaryCredentials =
        vendCredential("s3://storageBase/abc", Set.of(CredentialContext.Privilege.SELECT));
    assertThat(s3TemporaryCredentials.getAwsTempCredentials())
        .isEqualTo(
            new AwsCredentials()
                .accessKeyId(ACCESS_KEY)
                .secretAccessKey(SECRET_KEY)
                .sessionToken(SESSION_TOKEN));

    // Test when sts client is called
    when(serverProperties.getS3Configurations())
        .thenReturn(
            Map.of(
                NormalizedURL.from("s3://storageBase"),
                S3StorageConfig.builder()
                    .accessKey(ACCESS_KEY)
                    .secretKey(SECRET_KEY)
                    .region(S3_REGION)
                    .awsRoleArn(ROLE_ARN)
                    .build()));
    awsCredentialVendor = new AwsCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(awsCredentialVendor, null, null);
    assertThatThrownBy(
            () ->
                vendCredential("s3://storageBase/abc", Set.of(CredentialContext.Privilege.SELECT)))
        .isInstanceOf(StsException.class);
  }

  @Test
  public void testGenerateAzureTemporaryCredentials() {
    final String CLIENT_ID = "clientId";
    final String CLIENT_SECRET = "clientSecret";
    final String TENANT_ID = "tenantId";
    // Test mode used
    when(serverProperties.getAdlsConfigurations())
        .thenReturn(Map.of("uctest", ADLSStorageConfig.builder().testMode(true).build()));
    AzureCredentialVendor azureCredentialVendor = new AzureCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, azureCredentialVendor, null);
    TemporaryCredentials azureTemporaryCredentials =
        vendCredential(
            "abfss://test@uctest.dfs.core.windows.net", Set.of(CredentialContext.Privilege.UPDATE));
    assertThat(azureTemporaryCredentials.getAzureUserDelegationSas().getSasToken()).isNotNull();

    // Use datalake service client
    when(serverProperties.getAdlsConfigurations())
        .thenReturn(
            Map.of(
                "uctest",
                ADLSStorageConfig.builder()
                    .testMode(false)
                    .tenantId(TENANT_ID)
                    .clientId(CLIENT_ID)
                    .clientSecret(CLIENT_SECRET)
                    .build()));
    azureCredentialVendor = new AzureCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, azureCredentialVendor, null);
    assertThatThrownBy(
            () -> vendCredential("abfss://test@uctest", Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(CompletionException.class);
  }

  @Test
  public void testGenerateGcpTemporaryCredentials() {
    // Test mode using a static generator supplied by the test suite
    when(serverProperties.getGcsConfigurations())
        .thenReturn(
            Map.of(
                NormalizedURL.from("gs://uctest"),
                GcsStorageConfig.builder()
                    .bucketPath("gs://uctest")
                    .jsonKeyFilePath("")
                    .credentialGenerator(StaticTestingCredentialGenerator.class.getName())
                    .build()));
    GcpCredentialVendor gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, null, gcpCredentialVendor);
    TemporaryCredentials gcpTemporaryCredentials =
        vendCredential("gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE));
    assertThat(gcpTemporaryCredentials.getGcpOauthToken().getOauthToken()).isNotNull();

    // Testing shortcut using the legacy json key sentinel.
    final String testingSentinel = "testing://sentinel";
    when(serverProperties.getGcsConfigurations())
        .thenReturn(
            Map.of(
                NormalizedURL.from("gs://uctest"),
                GcsStorageConfig.builder()
                    .bucketPath("gs://uctest")
                    .jsonKeyFilePath(testingSentinel)
                    .credentialGenerator(TestingCredentialGenerator.class.getName())
                    .build()));
    gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, null, gcpCredentialVendor);
    TemporaryCredentials testingSentinelCredentials =
        vendCredential("gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.SELECT));
    assertThat(testingSentinelCredentials.getGcpOauthToken().getOauthToken())
        .isEqualTo(testingSentinel);

    // Use default creds (expected to fail without real GCP credentials)
    when(serverProperties.getGcsConfigurations())
        .thenReturn(
            Map.of(
                NormalizedURL.from("gs://uctest"),
                GcsStorageConfig.builder().bucketPath("gs://uctest").build()));
    gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, null, gcpCredentialVendor);
    assertThatThrownBy(
            () -> vendCredential("gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(BaseException.class);
  }

  @Test
  public void testMissingGcpBucketConfigurationFails() {
    when(serverProperties.getGcsConfigurations()).thenReturn(Map.of());
    GcpCredentialVendor gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, null, gcpCredentialVendor);
    assertThatThrownBy(
            () -> vendCredential("gs://missing/abc", Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Unknown GCS storage configuration");
  }

  @Test
  public void testVendCredentialWithExternalLocationCredential() {
    final String CREDENTIAL_ROLE_ARN = "arn:aws:iam::123456789012:role/external-location-role";
    final String S3_PATH = "s3://my-bucket/path/to/data";
    final String VENDED_ACCESS_KEY = "vendedAccessKey";
    final String VENDED_SECRET_KEY = "vendedSecretKey";
    final String VENDED_SESSION_TOKEN = "vendedSessionToken";

    // Create a CredentialDAO representing the credential associated with an external location
    CredentialDAO credentialDAO =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("test-credential")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn(CREDENTIAL_ROLE_ARN)),
            "test-user");
    String expectedExternalId = credentialDAO.getAwsIamRoleResponse().getExternalId();

    // Reset the mock to override the default stub from setUp()
    reset(externalLocationUtils);
    // Mock the external location utils to return this credential for the path
    doReturn(Optional.of(credentialDAO))
        .when(externalLocationUtils)
        .getExternalLocationCredentialDaoForPath(any());

    // No per-bucket configurations needed when using credential from external location
    when(serverProperties.getS3Configurations()).thenReturn(Map.of());
    // Return empty master config
    doReturn(S3StorageConfig.builder().build())
        .when(serverProperties)
        .getS3MasterRoleConfiguration();

    // Create a mock StsClient that captures the AssumeRoleRequest
    StsClient mockStsClient = Mockito.mock(StsClient.class);
    ArgumentCaptor<AssumeRoleRequest> requestCaptor =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Credentials stsCredentials =
        Credentials.builder()
            .accessKeyId(VENDED_ACCESS_KEY)
            .secretAccessKey(VENDED_SECRET_KEY)
            .sessionToken(VENDED_SESSION_TOKEN)
            .build();
    when(mockStsClient.assumeRole(requestCaptor.capture()))
        .thenReturn(AssumeRoleResponse.builder().credentials(stsCredentials).build());

    // Mock StsClient.builder() to return our mock StsClient
    StsClientBuilder mockBuilder = Mockito.mock(StsClientBuilder.class);
    when(mockBuilder.region(any())).thenReturn(mockBuilder);
    when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockStsClient);

    try (MockedStatic<StsClient> mockedStsClient = Mockito.mockStatic(StsClient.class)) {
      mockedStsClient.when(StsClient::builder).thenReturn(mockBuilder);

      // Create the AwsCredentialVendor - when it lazily initializes the master role generator,
      // it will use the mocked StsClient.builder()
      AwsCredentialVendor awsCredentialVendor = new AwsCredentialVendor(serverProperties);
      credentialsOperations = new CloudCredentialVendor(awsCredentialVendor, null, null);

      // Vend credentials - this should use the master role generator path
      TemporaryCredentials credentials =
          vendCredential(S3_PATH, Set.of(CredentialContext.Privilege.SELECT));

      // Verify the returned credentials match what the mock STS client returned
      assertThat(credentials.getAwsTempCredentials())
          .isEqualTo(
              new AwsCredentials()
                  .accessKeyId(VENDED_ACCESS_KEY)
                  .secretAccessKey(VENDED_SECRET_KEY)
                  .sessionToken(VENDED_SESSION_TOKEN));

      // Verify StsClient.assumeRole was called
      verify(mockStsClient).assumeRole(any(AssumeRoleRequest.class));

      // Verify the AssumeRoleRequest contains the correct roleArn and externalId
      // from the CredentialDAO associated with the external location
      AssumeRoleRequest capturedRequest = requestCaptor.getValue();
      assertThat(capturedRequest.roleArn()).isEqualTo(CREDENTIAL_ROLE_ARN);
      assertThat(capturedRequest.externalId()).isEqualTo(expectedExternalId);
    }
  }
}
