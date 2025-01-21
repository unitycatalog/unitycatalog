package io.unitycatalog.server.base;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.StsException;

public abstract class BaseCRUDTestWithMockCredentials extends BaseCRUDTest {
  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.put("s3.bucketPath.0", "s3://test-bucket0");
    serverProperties.put("s3.accessKey.0", "accessKey0");
    serverProperties.put("s3.secretKey.0", "secretKey0");
    serverProperties.put("s3.sessionToken.0", "sessionToken0");

    serverProperties.put("gcs.bucketPath.0", "gs://test-bucket0");
    serverProperties.put("gcs.jsonKeyFilePath.0", "testing://0");

    serverProperties.put("adls.storageAccountName.0", "test-bucket0");
    serverProperties.put("adls.tenantId.0", "tenantId0");
    serverProperties.put("adls.clientId.0", "clientId0");
    serverProperties.put("adls.clientSecret.0", "clientSecret0");
  }

  @Override
  protected void setUpCredentialOperations() {
    awsCredentialVendor = mock(AwsCredentialVendor.class);
    Credentials credentials =
        Credentials.builder()
            .accessKeyId("test-access-key-id")
            .secretAccessKey("test-secret-access-key")
            .sessionToken("test-session-token")
            .build();
    serverProperties.entrySet().stream()
        .filter(e -> e.getKey().toString().contains("s3.bucketPath"))
        .forEach(
            e ->
                doReturn(credentials)
                    .when(awsCredentialVendor)
                    .vendAwsCredentials(
                        argThat(
                            arg ->
                                arg.getStorageScheme().equals("s3")
                                    && arg.getStorageBase().contains(e.getValue().toString()))));

    azureCredentialVendor = mock(AzureCredentialVendor.class);
    AzureCredential azureCredential =
        AzureCredential.builder()
            .sasToken("test-sas-token")
            .expirationTimeInEpochMillis(System.currentTimeMillis() + 6000)
            .build();
    serverProperties.entrySet().stream()
        .filter(e -> e.getKey().toString().contains("adls.storageAccountName"))
        .forEach(
            e ->
                doReturn(azureCredential)
                    .when(azureCredentialVendor)
                    .vendAzureCredential(
                        argThat(
                            arg ->
                                arg.getStorageScheme().equals("abfs")
                                    && arg.getStorageBase().contains(e.getValue().toString()))));

    gcpCredentialVendor = mock(GcpCredentialVendor.class);
    AccessToken accessToken =
        new AccessToken("test-token", Date.from(Instant.now().plusSeconds(10 * 60)));
    serverProperties.entrySet().stream()
        .filter(e -> e.getKey().toString().contains("gcs.bucketPath"))
        .forEach(
            e ->
                doReturn(accessToken)
                    .when(gcpCredentialVendor)
                    .vendGcpToken(
                        argThat(
                            arg ->
                                arg.getStorageScheme().equals("gs")
                                    && arg.getStorageBase().contains(e.getValue().toString()))));

    credentialOperations =
        new CredentialOperations(awsCredentialVendor, azureCredentialVendor, gcpCredentialVendor);
  }

  protected String getTestCloudPath(String scheme, boolean isConfiguredPath) {
    String bucket = isConfiguredPath ? "test-bucket0" : "test-bucket1";
    return switch (scheme) {
        case "s3" -> "s3://" + bucket + "/test";
        case "abfs", "abfss" -> "abfs://test-container@" + bucket + ".dfs.core.windows.net/test";
        case "gs" -> "gs://" + bucket + "/test";
        default -> throw new IllegalArgumentException("Invalid scheme");
    };
  }

  protected void assertTemporaryCredentials(
      TemporaryCredentials temporaryCredentials, String scheme) {
    switch (scheme) {
      case "s3":
        assertThat(temporaryCredentials.getAwsTempCredentials()).isNotNull();
        assertThat(temporaryCredentials.getAwsTempCredentials().getSessionToken())
            .isEqualTo("test-session-token");
        assertThat(temporaryCredentials.getAwsTempCredentials().getAccessKeyId())
            .isEqualTo("test-access-key-id");
        assertThat(temporaryCredentials.getAwsTempCredentials().getSecretAccessKey())
            .isEqualTo("test-secret-access-key");
        break;
      case "abfs":
      case "abfss":
        assertThat(temporaryCredentials.getAzureUserDelegationSas()).isNotNull();
        assertThat(temporaryCredentials.getAzureUserDelegationSas().getSasToken())
            .isEqualTo("test-sas-token");
        break;
      case "gs":
        assertThat(temporaryCredentials.getGcpOauthToken()).isNotNull();
        assertThat(temporaryCredentials.getGcpOauthToken().getOauthToken()).isEqualTo("test-token");
        break;
      default:
        fail("Invalid scheme");
        break;
    }
  }

  protected void checkExceptionForScheme(Runnable functionCall, String scheme) {
    if ("s3".equals(scheme)) {
      assertThatThrownBy(functionCall::run, "Expected S3Exception for scheme: " + scheme)
          .isInstanceOf(StsException.class);
    } else if ("abfs".equals(scheme)) {
      assertThatThrownBy(functionCall::run, "Expected AbfsException for scheme: " + scheme)
          .isInstanceOf(CompletionException.class);
    } else if ("gs".equals(scheme)) {
      assertThatThrownBy(functionCall::run, "Expected GSException for scheme: " + scheme)
          .isInstanceOf(BaseException.class);
    } else {
      throw new IllegalArgumentException("Unsupported scheme: " + scheme);
    }
  }
  protected static Stream<Arguments> provideTestArguments() {
    List<String> clouds = List.of("s3", "abfs", "gs");
    List<Boolean> isConfiguredPathFlags = List.of(true, false);

    // Cartesian product of clouds and isConfiguredPathFlags
    return clouds.stream()
            .flatMap(cloud -> isConfiguredPathFlags.stream()
                    .map(isConfiguredPath -> Arguments.of(cloud, isConfiguredPath)));
  }
}
