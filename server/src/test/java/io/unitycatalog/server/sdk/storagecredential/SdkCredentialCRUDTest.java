package io.unitycatalog.server.sdk.storagecredential;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credential.BaseCredentialCRUDTest;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.externallocation.SdkExternalLocationOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Optional;
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

  // The CLI always sends a purpose, so only a request built through the client can leave it out.
  @Test
  public void testCreateCredentialWithoutPurposeIsRejected() throws ApiException {
    assertApiException(
        () ->
            credentialOperations.createCredential(
                new CreateCredentialRequest()
                    .name("uc_testcredential")
                    .awsIamRole(
                        new AwsIamRoleRequest()
                            .roleArn("arn:aws:iam::123456789012:role/role-name"))),
        ErrorCode.INVALID_ARGUMENT,
        "purpose is required.");
    assertThat(credentialOperations.listCredentials(Optional.empty(), CredentialPurpose.STORAGE))
        .isEmpty();
  }
}
