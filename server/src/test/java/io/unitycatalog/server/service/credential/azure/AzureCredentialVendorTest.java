package io.unitycatalog.server.service.credential.azure;

import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.AzureServicePrincipalRequest;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

public class AzureCredentialVendorTest {
  private static final NormalizedURL LOCATION =
      NormalizedURL.from("abfss://container@account.dfs.core.windows.net/path");
  private static final AzureServicePrincipalRequest SERVICE_PRINCIPAL =
      new AzureServicePrincipalRequest()
          .directoryId("11111111-1111-1111-1111-111111111111")
          .applicationId("22222222-2222-2222-2222-222222222222")
          .clientSecret("secret");

  private final List<AzureServicePrincipalRequest> signedInAs = new ArrayList<>();

  // No adls.* configuration, so the account is reachable only through a storage credential.
  private final AzureCredentialVendor vendor =
      new AzureCredentialVendor(
          new ServerProperties(new Properties()),
          servicePrincipal -> {
            signedInAs.add(servicePrincipal);
            return ctx ->
                AzureCredential.builder()
                    .sasToken("sas-for-" + servicePrincipal.getApplicationId())
                    .expirationTimeInEpochMillis(1L)
                    .build();
          });

  private static CredentialContext context(Optional<CredentialDAO> credential) {
    return CredentialContext.create(LOCATION, READ_ONLY, credential);
  }

  private static CredentialDAO credential(Consumer<CredentialDAO> setIdentity) {
    CredentialDAO credential = CredentialDAO.builder().id(UUID.randomUUID()).name("cred").build();
    setIdentity.accept(credential);
    return credential;
  }

  @Test
  public void testVendsWithTheServicePrincipalOfTheStorageCredential() {
    CredentialDAO credential = credential(c -> c.setAzureServicePrincipal(SERVICE_PRINCIPAL));

    AzureCredential vended = vendor.vendAzureCredential(context(Optional.of(credential)));

    assertThat(vended.getSasToken()).isEqualTo("sas-for-" + SERVICE_PRINCIPAL.getApplicationId());
    assertThat(signedInAs).containsExactly(SERVICE_PRINCIPAL);
  }

  @Test
  public void testRejectsAStorageCredentialOfAnotherCloud() {
    CredentialDAO credential =
        credential(
            c -> c.setAwsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::1:role/role")));

    assertThatThrownBy(() -> vendor.vendAzureCredential(context(Optional.of(credential))))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Storage credential 'cred' is not AZURE_SERVICE_PRINCIPAL")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.FAILED_PRECONDITION);
    assertThat(signedInAs).isEmpty();
  }

  @Test
  public void testWithoutAStorageCredentialTheAccountMustBeConfigured() {
    assertThatThrownBy(() -> vendor.vendAzureCredential(context(Optional.empty())))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Azure storage account configuration not found")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.FAILED_PRECONDITION);
    assertThat(signedInAs).isEmpty();
  }
}
