package io.unitycatalog.server.service.credential.gcp;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.time.Instant;
import java.util.Date;

/**
 * Test-only generator that issues a non-expiring static token.
 *
 * <p>Used by {@link io.unitycatalog.server.service.credential.CloudCredentialVendorTest} to verify
 * basic GCS credential vending without requiring real GCP service accounts.
 */
public class StaticTestingCredentialsGenerator implements GcpCredentialsGenerator {
  private final AccessToken staticToken;

  public StaticTestingCredentialsGenerator() {
    this.staticToken =
        AccessToken.newBuilder()
            .setTokenValue("testing://static-token")
            .setExpirationTime(Date.from(Instant.ofEpochMilli(Long.MAX_VALUE)))
            .build();
  }

  @Override
  public AccessToken generate(CredentialContext context) {
    return staticToken;
  }
}
