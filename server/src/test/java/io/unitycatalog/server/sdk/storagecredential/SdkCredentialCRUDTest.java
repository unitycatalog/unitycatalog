package io.unitycatalog.server.sdk.storagecredential;

import static io.unitycatalog.server.utils.TestUtils.generatePkcs8PrivateKeyPem;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credential.BaseCredentialCRUDTest;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.externallocation.SdkExternalLocationOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class SdkCredentialCRUDTest extends BaseCredentialCRUDTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected CredentialOperations createCredentialOperations(ServerConfig config) {
    return new SdkCredentialOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected ExternalLocationOperations createExternalLocationOperations(ServerConfig config) {
    return new SdkExternalLocationOperations(TestUtils.createApiClient(config));
  }

  /**
   * The generated response models have no field for a secret, so the shared CRUD tests cannot see
   * one. This reads the wire response instead.
   */
  @Test
  public void testGetAndListNeverReturnSecrets() throws Exception {
    String clientSecret = "secret-" + UUID.randomUUID();
    String privateKeyPem = generatePkcs8PrivateKeyPem();
    credentialOperations.createCredential(
        new CreateCredentialRequest()
            .name("uc_azure_raw")
            .purpose(CredentialPurpose.STORAGE)
            .azureServicePrincipal(azureServicePrincipal(clientSecret)));
    credentialOperations.createCredential(
        new CreateCredentialRequest()
            .name("uc_gcp_raw")
            .purpose(CredentialPurpose.STORAGE)
            .gcpServiceAccountKey(gcpServiceAccountKey(privateKeyPem)));

    String credentials = "/api/2.1/unity-catalog/credentials";
    // The identities are returned, so the secrets' absence below is not an empty response.
    assertThat(TestUtils.sendRawGet(serverConfig, credentials).body())
        .contains(DIRECTORY_ID, SERVICE_ACCOUNT_EMAIL);
    for (String path :
        List.of(credentials, credentials + "/uc_azure_raw", credentials + "/uc_gcp_raw")) {
      HttpResponse<String> response = TestUtils.sendRawGet(serverConfig, path);
      assertThat(response.statusCode()).as(path).isEqualTo(200);
      // "PRIVATE KEY" is the PEM header; private_key_id is expected, "private_key" alone is not.
      assertThat(response.body())
          .as(path)
          .doesNotContain(clientSecret, "client_secret", "PRIVATE KEY", "\"private_key\"");
    }
  }
}
