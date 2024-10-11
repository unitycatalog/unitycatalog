package io.unitycatalog.server.service.credential;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sts.model.StsException;

@ExtendWith(MockitoExtension.class)
public class CredentialOperationsTest {
  @Mock
  ServerProperties serverProperties;
  CredentialOperations credentialsOperations;

  @Test
  public void testGenerateS3TemporaryCredentials() {
    final String ACCESS_KEY = "accessKey";
    final String SECRET_KEY = "secretKey";
    final String SESSION_TOKEN = "sessionToken";
    final String S3_REGION = "us-west-2";
    final String ROLE_ARN = "roleArn";
    try (MockedStatic<ServerProperties> mockedStatic =
        mockStatic(ServerProperties.class)) {
      mockedStatic.when(ServerProperties::getInstance).thenReturn(serverProperties);
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
      credentialsOperations = new CredentialOperations();
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
      credentialsOperations = new CredentialOperations();
      assertThatThrownBy(
              () ->
                  credentialsOperations.vendCredential(
                      "s3://storageBase/abc", Set.of(CredentialContext.Privilege.SELECT)))
          .isInstanceOf(StsException.class);
    }
  }

  @Test
  public void testGenerateAzureTemporaryCredentials() {
    final String CLIENT_ID = "clientId";
    final String CLIENT_SECRET = "clientSecret";
    final String TENANT_ID = "tenantId";
    try (MockedStatic<ServerProperties> mockedStatic =
        mockStatic(ServerProperties.class)) {
      mockedStatic.when(ServerProperties::getInstance).thenReturn(serverProperties);
      // Test mode used
      when(serverProperties.getAdlsConfigurations())
          .thenReturn(Map.of("uctest", ADLSStorageConfig.builder().testMode(true).build()));
      credentialsOperations = new CredentialOperations();
      TemporaryCredentials azureTemporaryCredentials =
          credentialsOperations.vendCredential(
              "abfss://test@uctest.dfs.core.windows.net",
              Set.of(CredentialContext.Privilege.UPDATE));
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
      credentialsOperations = new CredentialOperations();
      assertThatThrownBy(
              () ->
                  credentialsOperations.vendCredential(
                      "abfss://test@uctest", Set.of(CredentialContext.Privilege.UPDATE)))
          .isInstanceOf(CompletionException.class);
    }
  }

  @Test
  public void testGenerateGcpTemporaryCredentials() {
    final String PROJECT_ID = "projectId";
    final String PRIVATE_KEY_ID = "privateKeyId";
    try (MockedStatic<ServerProperties> mockedStatic =
        mockStatic(ServerProperties.class)) {
      mockedStatic.when(ServerProperties::getInstance).thenReturn(serverProperties);
      // Test mode used
      when(serverProperties.getGcsConfigurations())
          .thenReturn(Map.of("gs://uctest", "testing://test"));
      credentialsOperations = new CredentialOperations();
      TemporaryCredentials gcpTemporaryCredentials =
          credentialsOperations.vendCredential(
              "gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE));
      assertThat(gcpTemporaryCredentials.getGcpOauthToken().getOauthToken()).isNotNull();

      // Use default creds
      when(serverProperties.getGcsConfigurations()).thenReturn(Map.of("gs://uctest", ""));
      credentialsOperations = new CredentialOperations();
      assertThatThrownBy(
              () ->
                  credentialsOperations.vendCredential(
                      "gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE)))
          .isInstanceOf(BaseException.class);
    }
  }
}
