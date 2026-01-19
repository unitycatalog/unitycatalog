package io.unitycatalog.server.service.credential.gcp;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.service.credential.CredentialContext;

/**
 * Pluggable contract for generating GCS OAuth tokens. Allows production code to load custom
 * implementations for integration tests without coupling to {@link GcpCredentialVendor}.
 */
public interface GcpCredentialsGenerator {
  AccessToken generate(CredentialContext context);
}
