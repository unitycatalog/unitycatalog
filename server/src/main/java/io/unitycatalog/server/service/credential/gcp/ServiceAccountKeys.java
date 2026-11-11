package io.unitycatalog.server.service.credential.gcp;

import com.google.auth.oauth2.ServiceAccountCredentials;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.GcpServiceAccountKeyRequest;
import java.io.IOException;

/** Turns the service account key of a storage credential into Google credentials. */
public final class ServiceAccountKeys {
  private ServiceAccountKeys() {}

  /**
   * Parses the key. The private key must be PEM-encoded PKCS#8, as in the {@code private_key} field
   * of a service account key JSON file.
   *
   * @throws BaseException with INVALID_ARGUMENT when the private key cannot be parsed, so a create
   *     or update request is rejected instead of storing a key that cannot vend
   */
  public static ServiceAccountCredentials parse(GcpServiceAccountKeyRequest key) {
    try {
      return ServiceAccountCredentials.fromPkcs8(
          /* clientId= */ null,
          key.getEmail(),
          key.getPrivateKey(),
          key.getPrivateKeyId(),
          /* scopes= */ null);
    } catch (IOException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "gcp_service_account_key.private_key is not a PEM-encoded PKCS#8 private key.",
          e);
    }
  }
}
