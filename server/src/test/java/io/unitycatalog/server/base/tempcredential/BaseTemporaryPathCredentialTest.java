package io.unitycatalog.server.base.tempcredential;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.*;

public abstract class BaseTemporaryPathCredentialTest extends BaseCRUDTestWithMockCredentials {
  protected TemporaryCredentialOperations temporaryCredentialOperations;

  protected abstract TemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    temporaryCredentialOperations = createTemporaryCredentialsOperations(serverConfig);
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "abfs", "gs"})
  public void testGenerateTemporaryPathCredentialsWhereConfIsProvided(String scheme) throws ApiException {
    String url = "";
    // test-bucket0 is configured in server properties
    switch (scheme) {
        case "s3" -> url = "s3://test-bucket0/test";
        case "abfs" -> url = "abfs://test-container@test-bucket0.dfs.core.windows.net/test";
        case "gs" -> url = "gs://test-bucket0/test";
        default -> fail("Invalid scheme");
    }
    GenerateTemporaryPathCredential generateTemporaryPathCredential =
        new GenerateTemporaryPathCredential().url(url).operation(PathOperation.PATH_READ);
    TemporaryCredentials temporaryCredentials =
        temporaryCredentialOperations.generateTemporaryPathCredentials(generateTemporaryPathCredential);

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
        assertThat(temporaryCredentials.getAzureUserDelegationSas()).isNotNull();
        assertThat(temporaryCredentials.getAzureUserDelegationSas().getSasToken())
                .isEqualTo("test-sas-token");
        break;
      case "gs":
        assertThat(temporaryCredentials.getGcpOauthToken()).isNotNull();
        assertThat(temporaryCredentials.getGcpOauthToken().getOauthToken())
                .isEqualTo("test-token");
        break;
      default:
        fail("Invalid scheme");
        break;
    }
  }
}
