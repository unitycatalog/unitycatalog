package io.unitycatalog.server.service.credential.gcp;

import static io.unitycatalog.server.utils.TestUtils.generatePkcs8PrivateKeyPem;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.auth.oauth2.ServiceAccountCredentials;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.GcpServiceAccountKeyRequest;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ServiceAccountKeysTest {
  private static final String EMAIL = "uc@my-project.iam.gserviceaccount.com";
  private static final String PRIVATE_KEY_ID = "0123456789abcdef0123456789abcdef01234567";

  private static GcpServiceAccountKeyRequest key(String privateKey) {
    return new GcpServiceAccountKeyRequest()
        .email(EMAIL)
        .privateKeyId(PRIVATE_KEY_ID)
        .privateKey(privateKey);
  }

  @Test
  public void testParsesTheKeyFileFields() {
    String pem = generatePkcs8PrivateKeyPem();

    ServiceAccountCredentials credentials = ServiceAccountKeys.parse(key(pem));

    assertThat(credentials.getClientEmail()).isEqualTo(EMAIL);
    assertThat(credentials.getPrivateKeyId()).isEqualTo(PRIVATE_KEY_ID);
    // The DER bytes of the parsed key are the PEM body, so the whole key was read.
    String body = pem.lines().filter(line -> !line.startsWith("-----")).reduce("", String::concat);
    assertThat(credentials.getPrivateKey().getEncoded())
        .isEqualTo(Base64.getDecoder().decode(body));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"not a key", "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----"})
  public void testRejectsTextThatIsNotAPrivateKey(String privateKey) {
    assertThatThrownBy(() -> ServiceAccountKeys.parse(key(privateKey)))
        .isInstanceOf(BaseException.class)
        .hasMessage("gcp_service_account_key.private_key is not a PEM-encoded PKCS#8 private key.")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);
  }
}
