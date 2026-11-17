package io.unitycatalog.server.service.credential.gcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.server.persist.secret.CredentialSecretStore;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Vends through the real {@link GcpCredentialVendor}, from a service account key created through
 * the API, with a generator in place of Google that returns what it was given.
 */
public class StoredGcpKeyVendingTest extends BaseCRUDTest {
  private static final String CREDENTIAL_NAME = "gcs_credential";
  private static final String EMAIL = "uc-test@my-project.iam.gserviceaccount.com";
  private static final String PRIVATE_KEY_ID = "0123456789abcdef";
  private static final String LOCATION = "gs://uc-test-bucket/data";

  /** The keys the vendor signed in with, as the generator received them. */
  private final List<GcpServiceAccountKeyRequest> keysSeen = new CopyOnWriteArrayList<>();

  private final String privateKey = TestUtils.generatePkcs8PrivateKeyPem();
  private TemporaryCredentialsApi temporaryCredentialsApi;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(
        ServerProperties.Property.CREDENTIAL_ENCRYPTION_KEY.getKey(),
        TestUtils.CREDENTIAL_ENCRYPTION_KEY);
  }

  @Override
  protected void setUpCredentialOperations(ServerProperties serverProperties) {
    GcpCredentialVendor gcp =
        new GcpCredentialVendor(
            serverProperties,
            CredentialSecretStore.from(serverProperties),
            key -> context -> echo(key, context));
    cloudCredentialVendor =
        new CloudCredentialVendor(
            new AwsCredentialVendor(serverProperties),
            new AzureCredentialVendor(serverProperties),
            gcp);
  }

  /** A token that says which key it was made for, with what permissions, for which paths. */
  private AccessToken echo(GcpServiceAccountKeyRequest key, CredentialContext context) {
    keysSeen.add(key);
    String value =
        String.join(
            " ",
            key.getEmail(),
            key.getPrivateKeyId(),
            context.getPrivileges().stream().map(Enum::name).sorted().toList().toString(),
            context.getLocations().toString());
    return new AccessToken(value, new Date(System.currentTimeMillis() + 600_000));
  }

  @BeforeEach
  @Override
  public void setUp() {
    // TODO(#2169): an encrypted key does not fit MySQL's tinytext column until it is widened.
    assumeFalse(
        String.valueOf(System.getenv("UC_TEST_DATABASE")).startsWith("MYSQL"),
        "the credential column is too small on MySQL until #2169");
    super.setUp();
    var apiClient = TestUtils.createApiClient(serverConfig);
    temporaryCredentialsApi = new TemporaryCredentialsApi(apiClient);
    try {
      new CredentialsApi(apiClient)
          .createCredential(
              new CreateCredentialRequest()
                  .name(CREDENTIAL_NAME)
                  .purpose(CredentialPurpose.STORAGE)
                  .gcpServiceAccountKey(
                      new io.unitycatalog.client.model.GcpServiceAccountKeyRequest()
                          .email(EMAIL)
                          .privateKeyId(PRIVATE_KEY_ID)
                          .privateKey(privateKey)));
      new ExternalLocationsApi(apiClient)
          .createExternalLocation(
              new CreateExternalLocation()
                  .name("gcs_location")
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
  public void testPathUnderLocationIsVendedFromTheStoredKey() throws ApiException {
    String path = LOCATION + "/table";
    TemporaryCredentials read = vend(path, PathOperation.PATH_READ);
    // The token is for the stored key, for reading, for the requested path only.
    assertThat(read.getGcpOauthToken().getOauthToken())
        .isEqualTo(EMAIL + " " + PRIVATE_KEY_ID + " [SELECT] [" + path + "]");
    assertThat(read.getExpirationTime()).isGreaterThan(System.currentTimeMillis());

    // Writing asks for the write role.
    assertThat(vend(path, PathOperation.PATH_READ_WRITE).getGcpOauthToken().getOauthToken())
        .contains("UPDATE");

    // The vendor signed in with the key as it was created: the stored copy decrypts to it.
    assertThat(keysSeen)
        .hasSize(2)
        .allSatisfy(
            key -> {
              assertThat(key.getEmail()).isEqualTo(EMAIL);
              assertThat(key.getPrivateKeyId()).isEqualTo(PRIVATE_KEY_ID);
              assertThat(key.getPrivateKey()).isEqualTo(privateKey);
            });
  }

  @Test
  public void testPathOutsideTheLocationIsNotVended() {
    assertThatThrownBy(() -> vend("gs://uc-test-bucket/other/table", PathOperation.PATH_READ))
        .isInstanceOf(ApiException.class);
    assertThat(keysSeen).isEmpty();
  }

  /**
   * Paths that look like they are under the location but are not, or that try to leave it. None may
   * be vended a token wider than the location. Most are refused; {@code %2e%2e} is accepted, since
   * the location covers the literal path, and the token is limited to that literal object name: GCS
   * does not resolve {@code ..}, so it cannot reach {@code other/}.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "gs://uc-test-bucket/data2/table",
        "gs://uc-test-bucket/data_archive/table",
        "gs://uc-test-bucket/dat",
        "gs://uc-test-bucket",
        "gs://uc-test-bucket/",
        "gs://uc-test-bucket/other/table",
        "gs://uc-test-bucket/data/../other/table",
        "gs://uc-test-bucket/data/%2e%2e/other/table",
        "gs://uc-test-bucket/data%2F..%2Fother/table",
        "gs://uc-test-bucket/data/./../other/table",
        "gs://other-bucket/data/table",
        "gs://uc-test-bucket.evil.com/data/table",
        "gs://user@uc-test-bucket/data/table",
        "gs://uc-test-bucket:80/data/table",
        "s3://uc-test-bucket/data/table",
        "abfss://data@uc-test-bucket.dfs.core.windows.net/table",
      })
  public void testPathsOutsideTheLocationAreNeverVended(String url) {
    try {
      TemporaryCredentials vended = vend(url, PathOperation.PATH_READ);
      // If a hostile path is accepted at all, the token must still be for a path inside data/.
      assertThat(vended.getGcpOauthToken().getOauthToken())
          .as("token for %s", url)
          .contains("[gs://uc-test-bucket/data/")
          .doesNotContain("/..");
    } catch (ApiException e) {
      // Refused, as it should be.
      assertThat(keysSeen).isEmpty();
    }
  }
}
