package io.unitycatalog.server.sdk.storagecredential;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.AzureServicePrincipalRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credential.BaseCredentialCRUDTest;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.externallocation.SdkExternalLocationOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class SdkCredentialCRUDTest extends BaseCredentialCRUDTest {
  private static final String CLIENT_SECRET = "azure-client-secret-value";

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

  private static AzureServicePrincipalRequest servicePrincipal() {
    return new AzureServicePrincipalRequest()
        .directoryId("directory-id")
        .applicationId("application-id")
        .clientSecret(CLIENT_SECRET);
  }

  private static GcpServiceAccountKeyRequest serviceAccountKey(String privateKey) {
    return new GcpServiceAccountKeyRequest()
        .email("uc@project.iam.gserviceaccount.com")
        .privateKeyId("key-id")
        .privateKey(privateKey);
  }

  private static CreateCredentialRequest request(String name) {
    return new CreateCredentialRequest().name(name).purpose(CredentialPurpose.STORAGE);
  }

  /**
   * Reads the raw responses rather than the SDK models, whose response types have no secret fields:
   * a secret in the JSON would be dropped by the client and go unnoticed.
   */
  @Test
  public void testGetAndListNeverReturnSecrets() throws Exception {
    String privateKey = TestUtils.generatePkcs8PrivateKeyPem();
    // A line of the key's base64 body, which JSON escaping leaves unchanged.
    String privateKeyLine = privateKey.split("\n")[1];
    credentialOperations.createCredential(
        request("azure_cred").azureServicePrincipal(servicePrincipal()));
    credentialOperations.createCredential(
        request("gcs_cred").gcpServiceAccountKey(serviceAccountKey(privateKey)));

    for (String path :
        List.of(
            "/api/2.1/unity-catalog/credentials/azure_cred",
            "/api/2.1/unity-catalog/credentials/gcs_cred",
            "/api/2.1/unity-catalog/credentials")) {
      HttpResponse<String> response = TestUtils.sendRawGet(serverConfig, path);
      assertThat(response.statusCode()).as(path).isEqualTo(200);
      assertThat(response.body())
          .as(path)
          .doesNotContain(CLIENT_SECRET)
          .doesNotContain("client_secret")
          .doesNotContain(privateKeyLine)
          .doesNotContain("\"private_key\"");
    }
  }

  @Test
  public void testCreateRejectsAnIncompleteOrAmbiguousIdentity() throws ApiException {
    String notExactlyOne =
        "Exactly one of aws_iam_role, azure_service_principal or gcp_service_account_key must"
            + " be set.";
    String validKey = TestUtils.generatePkcs8PrivateKeyPem();
    Map<CreateCredentialRequest, String> rejected =
        Map.of(
            request("no_identity"),
            notExactlyOne,
            request("two_identities")
                .azureServicePrincipal(servicePrincipal())
                .gcpServiceAccountKey(serviceAccountKey(validKey)),
            notExactlyOne,
            request("blank_role").awsIamRole(new AwsIamRoleRequest().roleArn(" ")),
            "aws_iam_role.role_arn must not be empty.",
            request("no_directory").azureServicePrincipal(servicePrincipal().directoryId(null)),
            "azure_service_principal.directory_id must not be empty.",
            request("no_application").azureServicePrincipal(servicePrincipal().applicationId("")),
            "azure_service_principal.application_id must not be empty.",
            request("no_secret").azureServicePrincipal(servicePrincipal().clientSecret(null)),
            "azure_service_principal.client_secret must not be empty.",
            request("no_email").gcpServiceAccountKey(serviceAccountKey(validKey).email("")),
            "gcp_service_account_key.email must not be empty.",
            request("no_key_id")
                .gcpServiceAccountKey(serviceAccountKey(validKey).privateKeyId(null)),
            "gcp_service_account_key.private_key_id must not be empty.",
            request("no_key").gcpServiceAccountKey(serviceAccountKey(null)),
            "gcp_service_account_key.private_key must not be empty.",
            request("bad_key").gcpServiceAccountKey(serviceAccountKey("not a key")),
            "gcp_service_account_key.private_key is not a PEM-encoded PKCS#8 private key.");
    rejected.forEach(
        (createRequest, message) ->
            assertApiException(
                () -> credentialOperations.createCredential(createRequest),
                ErrorCode.INVALID_ARGUMENT,
                message));
    assertThat(credentialOperations.listCredentials(Optional.empty(), CredentialPurpose.STORAGE))
        .isEmpty();
  }

  @Test
  public void testAwsIamRoleCannotBeSetOnAnAzureCredential() throws ApiException {
    CredentialInfo created =
        credentialOperations.createCredential(
            request("azure_cred").azureServicePrincipal(servicePrincipal()));
    assertApiException(
        () ->
            credentialOperations.updateCredential(
                "azure_cred",
                new UpdateCredentialRequest()
                    .awsIamRole(
                        new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/r"))),
        ErrorCode.INVALID_ARGUMENT,
        "Storage credential 'azure_cred' is not an AWS IAM role credential");
    assertThat(credentialOperations.getCredential("azure_cred")).isEqualTo(created);
  }
}
