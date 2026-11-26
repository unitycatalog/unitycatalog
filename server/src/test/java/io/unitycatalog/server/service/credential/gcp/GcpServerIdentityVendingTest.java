package io.unitycatalog.server.service.credential.gcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.oauth2.ComputeEngineCredentials;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.persist.secret.CredentialSecretStore;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.utils.GcpApplicationDefaultCredentials;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end API test for {@code gcp_server_identity} credentials: creates the credential without
 * an encryption key, attaches a {@code gs://} external location, and vends downscoped tokens
 * through the real {@link GcpCredentialVendor} backed by a mock GCE metadata/STS transport.
 */
public class GcpServerIdentityVendingTest extends BaseCRUDTest {
  private static final String CREDENTIAL_NAME = "gcs_server_identity";
  private static final String SERVER_EMAIL = "uc-server@my-project.iam.gserviceaccount.com";
  private static final String LOCATION = "gs://uc-ambient-bucket/data";

  private final List<String> stsBodies = new CopyOnWriteArrayList<>();
  private TemporaryCredentialsApi temporaryCredentialsApi;
  private CredentialInfo createdCredential;

  private final MockHttpTransport fakeGoogle =
      new MockHttpTransport() {
        @Override
        public LowLevelHttpRequest buildRequest(String method, String url) {
          return new MockLowLevelHttpRequest(url) {
            @Override
            public LowLevelHttpResponse execute() throws IOException {
              if (url.contains("/universe/universe-domain")) {
                return new MockLowLevelHttpResponse()
                    .setStatusCode(200)
                    .setContent("googleapis.com");
              }
              if (url.contains("/service-accounts/default/email")) {
                return new MockLowLevelHttpResponse().setStatusCode(200).setContent(SERVER_EMAIL);
              }
              if (url.contains("sts.googleapis.com")) {
                String body = URLDecoder.decode(getContentAsString(), StandardCharsets.UTF_8);
                stsBodies.add(body);
                String role =
                    body.contains("roles/storage.objectAdmin") ? "objectAdmin" : "objectViewer";
                return new MockLowLevelHttpResponse()
                    .setStatusCode(200)
                    .setContentType("application/json")
                    .setContent(
                        "{\"access_token\": \"downscoped-"
                            + role
                            + "\", \"expires_in\": 3600, \"token_type\": \"Bearer\","
                            + " \"issued_token_type\":"
                            + " \"urn:ietf:params:oauth:token-type:access_token\"}");
              }
              return new MockLowLevelHttpResponse()
                  .setStatusCode(200)
                  .setContentType("application/json")
                  .setContent(
                      "{\"access_token\": \"metadata-source-token\", \"expires_in\": 3600,"
                          + " \"token_type\": \"Bearer\"}");
            }
          };
        }
      };

  private final GcpApplicationDefaultCredentials adc =
      () -> ComputeEngineCredentials.newBuilder().setHttpTransportFactory(() -> fakeGoogle).build();

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // Deliberately no server.credential-encryption-key set: gcp_server_identity needs none.
    gcpApplicationDefaultCredentials = adc;
  }

  @Override
  protected void setUpCredentialOperations(ServerProperties serverProperties) {
    GcpCredentialVendor gcp =
        new GcpCredentialVendor(
            serverProperties,
            CredentialSecretStore.from(serverProperties),
            null,
            adc,
            () -> fakeGoogle);
    cloudCredentialVendor =
        new CloudCredentialVendor(
            new AwsCredentialVendor(serverProperties),
            new AzureCredentialVendor(serverProperties),
            gcp);
  }

  @BeforeEach
  @Override
  public void setUp() {
    stsBodies.clear();
    super.setUp();
    var apiClient = TestUtils.createApiClient(serverConfig);
    temporaryCredentialsApi = new TemporaryCredentialsApi(apiClient);
    try {
      createdCredential =
          new CredentialsApi(apiClient)
              .createCredential(
                  new CreateCredentialRequest()
                      .name(CREDENTIAL_NAME)
                      .purpose(CredentialPurpose.STORAGE)
                      .gcpServerIdentity(Map.of()));
      new ExternalLocationsApi(apiClient)
          .createExternalLocation(
              new CreateExternalLocation()
                  .name("gcs_ambient_location")
                  .url(LOCATION)
                  .credentialName(CREDENTIAL_NAME));
    } catch (ApiException e) {
      throw new IllegalStateException(e);
    }
  }

  private TemporaryCredentials vend(String url, PathOperation operation) throws ApiException {
    return temporaryCredentialsApi.generateTemporaryPathCredentials(
        new GenerateTemporaryPathCredential().url(url).operation(operation));
  }

  @Test
  public void testCreateReturnsServerEmailAndVendsDownscopedTokens() throws ApiException {
    assertThat(createdCredential.getGcpServerIdentity().getUnityCatalogServiceAccountEmail())
        .isEqualTo(SERVER_EMAIL);

    String path = LOCATION + "/table";
    TemporaryCredentials read = vend(path, PathOperation.PATH_READ);
    assertThat(read.getGcpOauthToken().getOauthToken()).isEqualTo("downscoped-objectViewer");
    assertThat(read.getExpirationTime()).isGreaterThan(System.currentTimeMillis());

    TemporaryCredentials write = vend(path, PathOperation.PATH_READ_WRITE);
    assertThat(write.getGcpOauthToken().getOauthToken()).isEqualTo("downscoped-objectAdmin");

    assertThat(stsBodies).hasSize(2);
    assertThat(stsBodies.get(0))
        .contains("metadata-source-token")
        .contains("projects/_/buckets/uc-ambient-bucket")
        .contains("objects/data/table")
        .contains("roles/storage.objectViewer");
    assertThat(stsBodies.get(1))
        .contains("metadata-source-token")
        .contains("projects/_/buckets/uc-ambient-bucket")
        .contains("objects/data/table")
        .contains("roles/storage.objectAdmin");
  }

  @Test
  public void testPathOutsideLocationIsRefused() {
    assertThatThrownBy(() -> vend("gs://uc-ambient-bucket/other/table", PathOperation.PATH_READ))
        .isInstanceOf(ApiException.class)
        .extracting(e -> ((ApiException) e).getCode())
        .isEqualTo(400);
    assertThat(stsBodies).isEmpty();
  }
}
