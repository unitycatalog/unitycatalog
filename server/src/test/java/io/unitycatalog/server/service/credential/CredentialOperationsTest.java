package io.unitycatalog.server.service.credential;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sts.model.StsException;

@ExtendWith(MockitoExtension.class)
public class CredentialOperationsTest {
  @Mock ServerProperties serverProperties;
  CredentialOperations credentialsOperations;

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
    credentialsOperations = new CredentialOperations(awsCredentialVendor, null, null);
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
    credentialsOperations = new CredentialOperations(awsCredentialVendor, null, null);
    assertThatThrownBy(
            () ->
                credentialsOperations.vendCredential(
                    "s3://storageBase/abc", Set.of(CredentialContext.Privilege.SELECT)))
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
    credentialsOperations = new CredentialOperations(null, azureCredentialVendor, null);
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
    credentialsOperations = new CredentialOperations(null, azureCredentialVendor, null);
    assertThatThrownBy(
            () ->
                credentialsOperations.vendCredential(
                    "abfss://test@uctest", Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(CompletionException.class);
  }

  @Test
  public void testGenerateGcpTemporaryCredentials() {
    // Test mode used
    when(serverProperties.getGcsConfigurations())
        .thenReturn(Map.of("gs://uctest", "testing://test"));
    GcpCredentialVendor gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CredentialOperations(null, null, gcpCredentialVendor);
    TemporaryCredentials gcpTemporaryCredentials =
        credentialsOperations.vendCredential(
            "gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE));
    assertThat(gcpTemporaryCredentials.getGcpOauthToken().getOauthToken()).isNotNull();

    // Use default creds
    when(serverProperties.getGcsConfigurations()).thenReturn(Map.of("gs://uctest", ""));
    gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CredentialOperations(null, null, gcpCredentialVendor);
    assertThatThrownBy(
            () ->
                credentialsOperations.vendCredential(
                    "gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(BaseException.class);
  }
}
