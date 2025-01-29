package io.unitycatalog.server.base;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import software.amazon.awssdk.services.sts.model.Credentials;

public abstract class BaseCRUDTestWithMockCredentials extends BaseCRUDTest {
  @Mock AwsCredentialVendor awsCredentialVendor;
  @Mock AzureCredentialVendor azureCredentialVendor;
  @Mock GcpCredentialVendor gcpCredentialVendor;

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
    setupAwsCredentials();
    setupAzureCredentials();
    setupGcpCredentials();

    credentialOperations = new CredentialOperations(
            awsCredentialVendor,
            azureCredentialVendor,
            gcpCredentialVendor
    );
  }

  private void setupAwsCredentials() {
    awsCredentialVendor = mock(AwsCredentialVendor.class);
    Credentials awsCredential = Credentials.builder()
            .accessKeyId("test-access-key-id")
            .secretAccessKey("test-secret-access-key")
            .sessionToken("test-session-token")
            .build();
    serverProperties.entrySet().stream()
            .filter(e -> e.getKey().toString().startsWith("s3.bucketPath"))
            .map(e -> e.getValue().toString())
            .forEach(path -> doReturn(awsCredential)
                    .when(awsCredentialVendor)
                    .vendAwsCredentials(
                            argThat(isCredentialContextForCloudPath("s3", path))));
  }

  private void setupAzureCredentials() {
    azureCredentialVendor = mock(AzureCredentialVendor.class);
    AzureCredential azureCredential = AzureCredential.builder()
            .sasToken("test-sas-token")
            .expirationTimeInEpochMillis(System.currentTimeMillis() + 6000)
            .build();
    serverProperties.entrySet().stream()
            .filter(e -> e.getKey().toString().startsWith("adls.storageAccountName"))
            .map(e -> e.getValue().toString())
            .forEach(path -> doReturn(azureCredential)
                    .when(azureCredentialVendor)
                    .vendAzureCredential(
                            argThat(isCredentialContextForCloudPath("abfs", path))));
  }

  private void setupGcpCredentials() {
    gcpCredentialVendor = mock(GcpCredentialVendor.class);
    AccessToken gcpCredential = new AccessToken(
            "test-token",
            Date.from(Instant.now().plusSeconds(10 * 60))
    );
    serverProperties.entrySet().stream()
            .filter(e -> e.getKey().toString().startsWith("gcs.bucketPath"))
            .map(e -> e.getValue().toString())
            .forEach(path -> doReturn(gcpCredential)
                    .when(gcpCredentialVendor)
                    .vendGcpToken(
                            argThat(isCredentialContextForCloudPath("gs", path))));
  }

  private ArgumentMatcher<CredentialContext> isCredentialContextForCloudPath(String scheme, String path) {
    return arg -> arg.getStorageScheme().equals(scheme) && arg.getStorageBase().contains(path);
  }

  /**
   * @param scheme s3, abfs, gs
   * @param isConfiguredPath true if the path is configured in the server properties
   * @return Cloud path for testing
   */
  protected String getTestCloudPath(String scheme, boolean isConfiguredPath) {
    // test-bucket0 is configured in the properties
    String bucket = isConfiguredPath ? "test-bucket0" : "test-bucket1";
    return switch (scheme) {
        case "s3" -> "s3://" + bucket + "/test";
        case "abfs", "abfss" -> "abfs://test-container@" + bucket + ".dfs.core.windows.net/test";
        case "gs" -> "gs://" + bucket + "/test";
        default -> throw new IllegalArgumentException("Invalid scheme");
    };
  }

  protected void assertTemporaryCredentials(
      TemporaryCredentials tempCredentials, String scheme) {
    switch (scheme) {
      case "s3":
        AwsCredentials awsCredentials = tempCredentials.getAwsTempCredentials();
        assertThat(awsCredentials).isNotNull();
        assertThat(awsCredentials.getSessionToken()).isEqualTo("test-session-token");
        assertThat(awsCredentials.getAccessKeyId()).isEqualTo("test-access-key-id");
        assertThat(awsCredentials.getSecretAccessKey()).isEqualTo("test-secret-access-key");
        break;
      case "abfs":
      case "abfss":
        AzureUserDelegationSAS azureUserDelegationSAS = tempCredentials.getAzureUserDelegationSas();
        assertThat(azureUserDelegationSAS).isNotNull();
        assertThat(azureUserDelegationSAS.getSasToken()).isEqualTo("test-sas-token");
        break;
      case "gs":
        GcpOauthToken gcpOauthToken = tempCredentials.getGcpOauthToken();
        assertThat(gcpOauthToken).isNotNull();
        assertThat(gcpOauthToken.getOauthToken()).isEqualTo("test-token");
        break;
      default:
        fail("Invalid scheme");
        break;
    }
  }

  /**
   * isConfiguredPath is true if the path is configured in the server properties
   *
   * @return Stream of arguments (s3, abfs, gs) x isConfiguredPath (true, false) for testing
   */
  protected static Stream<Arguments> getArgumentsForParameterizedTests() {
    List<String> clouds = List.of("s3", "abfs", "gs");
    List<Boolean> isConfiguredPathFlags = List.of(true, false);

    // Cartesian product of clouds and isConfiguredPathFlags
    return clouds.stream()
            .flatMap(cloud -> isConfiguredPathFlags.stream()
                    .map(isConfiguredPath -> Arguments.of(cloud, isConfiguredPath)));
  }
}
