package io.unitycatalog.server.service.credential;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.aws.CredentialsGenerator;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcsStorageConfig;
import io.unitycatalog.server.service.credential.gcp.StaticTestingCredentialsGenerator;
import io.unitycatalog.server.service.credential.gcp.TestingCredentialsGenerator;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.StsException;

@ExtendWith(MockitoExtension.class)
public class CloudCredentialVendorTest {
  @Mock ServerProperties serverProperties;
  @Mock CredentialsGenerator.StsCredentialsGenerator stsCredentialsGenerator;
  CloudCredentialVendor credentialsOperations;

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
                "s3://storageBase",
                S3StorageConfig.builder()
                    .accessKey(ACCESS_KEY)
                    .secretKey(SECRET_KEY)
                    .sessionToken(SESSION_TOKEN)
                    .build()));
    AwsCredentialVendor awsCredentialVendor = new AwsCredentialVendor(serverProperties);
    credentialsOperations =
        new CloudCredentialVendor(awsCredentialVendor, null, null, serverProperties);
    TemporaryCredentials s3TemporaryCredentials =
        credentialsOperations.vendCredential(
            "s3://storageBase/abc", Set.of(CredentialContext.Privilege.SELECT));
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
                "s3://storageBase",
                S3StorageConfig.builder()
                    .accessKey(ACCESS_KEY)
                    .secretKey(SECRET_KEY)
                    .region(S3_REGION)
                    .awsRoleArn(ROLE_ARN)
                    .build()));
    awsCredentialVendor = new AwsCredentialVendor(serverProperties);
    credentialsOperations =
        new CloudCredentialVendor(awsCredentialVendor, null, null, serverProperties);
    assertThatThrownBy(
            () ->
                credentialsOperations.vendCredential(
                    "s3://storageBase/abc", Set.of(CredentialContext.Privilege.SELECT)))
        .isInstanceOf(StsException.class);
  }

  @Test
  public void testGenerateS3TemporaryCredentialsWithEndpointUrl() {
    final String ACCESS_KEY = "accessKey";
    final String SECRET_KEY = "secretKey";
    final String SESSION_TOKEN = "sessionToken";
    final String S3_REGION = "us-west-2";
    final String ENDPOINT_URL = "http://localhost";
    when(serverProperties.getS3Configurations())
        .thenReturn(
            Map.of(
                "s3://storageBase",
                S3StorageConfig.builder()
                    .accessKey(ACCESS_KEY)
                    .secretKey(SECRET_KEY)
                    .region(S3_REGION)
                    .endpointUrl(ENDPOINT_URL)
                    .build()));
    when(stsCredentialsGenerator.generate(any()))
        .thenReturn(
            Credentials.builder()
                .accessKeyId(ACCESS_KEY)
                .secretAccessKey(SECRET_KEY)
                .sessionToken(SESSION_TOKEN)
                .build());
    AwsCredentialVendor awsCredentialVendor = new MockedAwsCredentialVendor(serverProperties);
    credentialsOperations =
        new CloudCredentialVendor(awsCredentialVendor, null, null, serverProperties);
    TemporaryCredentials s3TemporaryCredentials =
        credentialsOperations.vendCredential(
            "s3://storageBase/abc", Set.of(CredentialContext.Privilege.SELECT));
    assertThat(s3TemporaryCredentials.getAwsTempCredentials())
        .isEqualTo(
            new AwsCredentials()
                .accessKeyId(ACCESS_KEY)
                .secretAccessKey(SECRET_KEY)
                .sessionToken(SESSION_TOKEN));
    assertThat(s3TemporaryCredentials.getEndpointUrl()).isEqualTo(ENDPOINT_URL);
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
    credentialsOperations =
        new CloudCredentialVendor(null, azureCredentialVendor, null, serverProperties);
    TemporaryCredentials azureTemporaryCredentials =
        credentialsOperations.vendCredential(
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
    credentialsOperations =
        new CloudCredentialVendor(null, azureCredentialVendor, null, serverProperties);
    assertThatThrownBy(
            () ->
                credentialsOperations.vendCredential(
                    "abfss://test@uctest", Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(CompletionException.class);
  }

  @Test
  public void testGenerateGcpTemporaryCredentials() {
    // Test mode using a static generator supplied by the test suite
    when(serverProperties.getGcsConfigurations())
        .thenReturn(
            Map.of(
                "gs://uctest",
                GcsStorageConfig.builder()
                    .bucketPath("gs://uctest")
                    .jsonKeyFilePath("")
                    .credentialsGenerator(StaticTestingCredentialsGenerator.class.getName())
                    .build()));
    GcpCredentialVendor gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations =
        new CloudCredentialVendor(null, null, gcpCredentialVendor, serverProperties);
    TemporaryCredentials gcpTemporaryCredentials =
        credentialsOperations.vendCredential(
            "gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE));
    assertThat(gcpTemporaryCredentials.getGcpOauthToken().getOauthToken()).isNotNull();

    // Testing shortcut using the legacy json key sentinel.
    final String testingSentinel = "testing://sentinel";
    when(serverProperties.getGcsConfigurations())
        .thenReturn(
            Map.of(
                "gs://uctest",
                GcsStorageConfig.builder()
                    .bucketPath("gs://uctest")
                    .jsonKeyFilePath(testingSentinel)
                    .credentialsGenerator(TestingCredentialsGenerator.class.getName())
                    .build()));
    gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, null, gcpCredentialVendor);
    TemporaryCredentials testingSentinelCredentials =
        credentialsOperations.vendCredential(
            "gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.SELECT));
    assertThat(testingSentinelCredentials.getGcpOauthToken().getOauthToken())
        .isEqualTo(testingSentinel);

    // Use default creds (expected to fail without real GCP credentials)
    when(serverProperties.getGcsConfigurations())
        .thenReturn(
            Map.of("gs://uctest", GcsStorageConfig.builder().bucketPath("gs://uctest").build()));
    gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations =
        new CloudCredentialVendor(null, null, gcpCredentialVendor, serverProperties);
    assertThatThrownBy(
            () ->
                credentialsOperations.vendCredential(
                    "gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(BaseException.class);
  }

  @Test
  public void testMissingGcpBucketConfigurationFails() {
    when(serverProperties.getGcsConfigurations()).thenReturn(Map.of());
    GcpCredentialVendor gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, null, gcpCredentialVendor);
    assertThatThrownBy(
            () ->
                credentialsOperations.vendCredential(
                    "gs://missing/abc", Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Unknown GCS storage configuration");
  }

  class MockedAwsCredentialVendor extends AwsCredentialVendor {

    MockedAwsCredentialVendor(ServerProperties serverProperties) {
      super(serverProperties);
    }

    @Override
    protected CredentialsGenerator createCredentialsGenerator(S3StorageConfig config) {
      return stsCredentialsGenerator;
    }
  }
}
