package io.unitycatalog.server.sdk.tempcredential;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.AzureServicePrincipalRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Vends temporary credentials for paths under external locations whose credential is an Azure
 * service principal or a GCS service account key. The real vendors run with generators that echo
 * what they were given instead of calling Azure or Google.
 */
public class SdkStoredCredentialVendingTest extends BaseCRUDTest {
  private static final String AZURE_LOCATION =
      "abfss://container@account.dfs.core.windows.net/data";
  private static final String GCS_LOCATION = "gs://bucket/data";
  private static final long EXPIRATION_MS = 1_900_000_000_000L;

  /** The identities, secrets included, that the fake generators were made with, in order. */
  private final List<String> servicePrincipalsSeen = new CopyOnWriteArrayList<>();

  private final List<String> keysSeen = new CopyOnWriteArrayList<>();

  private CredentialsApi credentialsApi;
  private ExternalLocationsApi externalLocationsApi;
  private TemporaryCredentialsApi temporaryCredentialsApi;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected void setUpCredentialOperations(ServerProperties serverProperties) {
    AzureCredentialVendor azure =
        new AzureCredentialVendor(
            serverProperties,
            servicePrincipal -> {
              servicePrincipalsSeen.add(
                  String.join(
                      " ",
                      servicePrincipal.getDirectoryId(),
                      servicePrincipal.getApplicationId(),
                      servicePrincipal.getClientSecret()));
              return context ->
                  AzureCredential.builder()
                      .sasToken(echo(servicePrincipal.getApplicationId(), context))
                      .expirationTimeInEpochMillis(EXPIRATION_MS)
                      .build();
            });
    GcpCredentialVendor gcp =
        new GcpCredentialVendor(
            serverProperties,
            key -> {
              keysSeen.add(
                  String.join(" ", key.getEmail(), key.getPrivateKeyId(), key.getPrivateKey()));
              return context ->
                  new AccessToken(echo(key.getEmail(), context), new Date(EXPIRATION_MS));
            });
    cloudCredentialVendor =
        new CloudCredentialVendor(new AwsCredentialVendor(serverProperties), azure, gcp);
  }

  private static String echo(String identity, CredentialContext context) {
    return identity + " " + new TreeSet<>(context.getPrivileges()) + " " + context.getLocations();
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    credentialsApi = new CredentialsApi(TestUtils.createApiClient(serverConfig));
    externalLocationsApi = new ExternalLocationsApi(TestUtils.createApiClient(serverConfig));
    temporaryCredentialsApi = new TemporaryCredentialsApi(TestUtils.createApiClient(serverConfig));
  }

  private TemporaryCredentials vend(String url, PathOperation operation) throws ApiException {
    return temporaryCredentialsApi.generateTemporaryPathCredentials(
        new GenerateTemporaryPathCredential().url(url).operation(operation));
  }

  @Test
  public void testAzureServicePrincipalVendsSasTokens() throws ApiException {
    credentialsApi.createCredential(
        new CreateCredentialRequest()
            .name("azure_cred")
            .purpose(CredentialPurpose.STORAGE)
            .azureServicePrincipal(
                new AzureServicePrincipalRequest()
                    .directoryId("directory-id")
                    .applicationId("application-id")
                    .clientSecret("client-secret")));
    externalLocationsApi.createExternalLocation(
        new CreateExternalLocation()
            .name("azure_loc")
            .url(AZURE_LOCATION)
            .credentialName("azure_cred"));

    String table = AZURE_LOCATION + "/table";
    TemporaryCredentials read = vend(table, PathOperation.PATH_READ);
    assertThat(read.getUrl()).isEqualTo(table);
    assertThat(read.getAzureUserDelegationSas().getSasToken())
        .isEqualTo("application-id [SELECT] [" + table + "]");
    assertThat(read.getExpirationTime()).isEqualTo(EXPIRATION_MS);
    assertThat(vend(table, PathOperation.PATH_READ_WRITE).getAzureUserDelegationSas().getSasToken())
        .isEqualTo("application-id [SELECT, UPDATE] [" + table + "]");
    // Each vend signed in with the stored service principal, client secret included.
    assertThat(servicePrincipalsSeen)
        .containsExactly(
            "directory-id application-id client-secret",
            "directory-id application-id client-secret");
  }

  @Test
  public void testGcsServiceAccountKeyVendsTokens() throws ApiException {
    String privateKey = TestUtils.generatePkcs8PrivateKeyPem();
    credentialsApi.createCredential(
        new CreateCredentialRequest()
            .name("gcs_cred")
            .purpose(CredentialPurpose.STORAGE)
            .gcpServiceAccountKey(
                new GcpServiceAccountKeyRequest()
                    .email("uc@project.iam.gserviceaccount.com")
                    .privateKeyId("key-id")
                    .privateKey(privateKey)));
    externalLocationsApi.createExternalLocation(
        new CreateExternalLocation().name("gcs_loc").url(GCS_LOCATION).credentialName("gcs_cred"));

    String table = GCS_LOCATION + "/table";
    TemporaryCredentials read = vend(table, PathOperation.PATH_READ);
    assertThat(read.getUrl()).isEqualTo(table);
    assertThat(read.getGcpOauthToken().getOauthToken())
        .isEqualTo("uc@project.iam.gserviceaccount.com [SELECT] [" + table + "]");
    assertThat(read.getExpirationTime()).isEqualTo(EXPIRATION_MS);
    // The vend signed in with the stored key, private key included.
    assertThat(keysSeen).containsExactly("uc@project.iam.gserviceaccount.com key-id " + privateKey);
  }
}
