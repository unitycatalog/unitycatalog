package io.unitycatalog.server.sdk.storagecredential;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.client.model.S3CompatibleRequest;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A server that only has Google Cloud credentials: it has the key that encrypts stored secrets, but
 * the experimental S3-compatible storage stays off, as in the Helm chart with only the Secret set.
 * The credential tests of {@link io.unitycatalog.server.base.credential.BaseCredentialCRUDTest}
 * turn S3-compatible storage on, so they do not cover this.
 */
public class GcpOnlyServerCredentialTest extends BaseServerTest {
  private CredentialOperations credentialOperations;
  private final AtomicReference<IOException> adcFailure = new AtomicReference<>();

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(
        Property.CREDENTIAL_ENCRYPTION_KEY.getKey(), TestUtils.CREDENTIAL_ENCRYPTION_KEY);
    gcpApplicationDefaultCredentials =
        () -> {
          IOException failure = adcFailure.get();
          if (failure != null) {
            throw failure;
          }
          return GoogleCredentials.create(new AccessToken("token", null));
        };
  }

  @BeforeEach
  @Override
  public void setUp() {
    adcFailure.set(null);
    super.setUp();
    credentialOperations = new SdkCredentialOperations(TestUtils.createApiClient(serverConfig));
  }

  @Test
  public void testGcpCredentialWorksWhileS3CompatibleStorageIsOff() throws ApiException {
    CredentialInfo created =
        credentialOperations.createCredential(
            new CreateCredentialRequest()
                .name("gcp_cred")
                .purpose(CredentialPurpose.STORAGE)
                .gcpServiceAccountKey(
                    new GcpServiceAccountKeyRequest()
                        .email("uc-test@my-project.iam.gserviceaccount.com")
                        .privateKeyId("0123456789abcdef")
                        .privateKey(TestUtils.generatePkcs8PrivateKeyPem())));
    assertThat(credentialOperations.getCredential("gcp_cred")).isEqualTo(created);

    CredentialInfo ambient =
        credentialOperations.createCredential(
            new CreateCredentialRequest()
                .name("gcp_ambient_cred")
                .purpose(CredentialPurpose.STORAGE)
                .gcpServerIdentity(Map.of()));
    assertThat(credentialOperations.getCredential("gcp_ambient_cred")).isEqualTo(ambient);

    adcFailure.set(new IOException("Your default credentials were not found."));
    assertApiException(
        () ->
            credentialOperations.createCredential(
                new CreateCredentialRequest()
                    .name("gcp_ambient_missing_adc")
                    .purpose(CredentialPurpose.STORAGE)
                    .gcpServerIdentity(Map.of())),
        ErrorCode.FAILED_PRECONDITION,
        "Application Default Credentials");

    assertApiException(
        () ->
            credentialOperations.createCredential(
                new CreateCredentialRequest()
                    .name("s3_cred")
                    .purpose(CredentialPurpose.STORAGE)
                    .s3Compatible(
                        new S3CompatibleRequest()
                            .accessKeyId("key-id-1")
                            .secretAccessKey("key-value-1")
                            .s3Endpoint("http://s3:9000"))),
        ErrorCode.INVALID_ARGUMENT,
        "currently disabled");
  }
}
