package io.unitycatalog.server.utils;

import com.google.auth.oauth2.ServiceAccountCredentials;
import io.unitycatalog.server.model.GcpServiceAccountKeyRequest;
import java.io.IOException;

/** Loads Google Cloud service account keys. */
public final class GcpServiceAccountKeys {
  private GcpServiceAccountKeys() {}

  /**
   * @return the credentials of a service account key
   * @throws IOException if the private key is not a PEM-encoded PKCS#8 key
   */
  public static ServiceAccountCredentials credentialsOf(GcpServiceAccountKeyRequest key)
      throws IOException {
    return ServiceAccountCredentials.fromPkcs8(
        /* clientId= */ null,
        key.getEmail(),
        key.getPrivateKey(),
        key.getPrivateKeyId(),
        /* scopes= */ null);
  }
}
