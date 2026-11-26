package io.unitycatalog.server.utils;

import com.google.auth.ServiceAccountSigner;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.util.Optional;

/**
 * Loads the server's Google Cloud Application Default Credentials and resolves its service account
 * email when the credentials identify one.
 */
@FunctionalInterface
public interface GcpApplicationDefaultCredentials {
  /** Default implementation backed by {@link GoogleCredentials#getApplicationDefault()}. */
  GcpApplicationDefaultCredentials DEFAULT = GoogleCredentials::getApplicationDefault;

  /**
   * Loads the server's Google Cloud Application Default Credentials.
   *
   * @return the server's default Google credentials
   * @throws IOException if Application Default Credentials are not configured or cannot be loaded
   */
  GoogleCredentials load() throws IOException;

  /**
   * Returns the service account email of {@code credentials}, if they identify a service account.
   *
   * <p>Service-account-backed credentials ({@code ComputeEngineCredentials}, {@code
   * ServiceAccountCredentials}, {@code ImpersonatedCredentials}) implement {@link
   * ServiceAccountSigner} and return their account email; {@link ExternalAccountCredentials}
   * returns its impersonated service account email when configured. Other credential types (such as
   * user credentials) return empty.
   *
   * @param credentials the loaded Google credentials
   * @return the service account email, or empty if the credentials do not identify one
   */
  static Optional<String> serviceAccountEmail(GoogleCredentials credentials) {
    String email = null;
    if (credentials instanceof ServiceAccountSigner signer) {
      email = signer.getAccount();
    } else if (credentials instanceof ExternalAccountCredentials external) {
      email = external.getServiceAccountEmail();
    }
    return email == null || email.isBlank() ? Optional.empty() : Optional.of(email);
  }
}
