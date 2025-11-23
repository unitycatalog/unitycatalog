package io.unitycatalog.server.service.credential.gcp;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.time.Instant;
import java.util.Date;

/**
 * Simple generator that returns a static token for GCS. Intended for testing and local development
 * scenarios where a real GCP service account is not available.
 */
public class TestingCredentialsGenerator implements GcpCredentialsGenerator {
  private static final long TOKEN_EXPIRATION_MILLIS = 253370790000000L;

  private final AccessToken staticToken;

  public TestingCredentialsGenerator(GcsStorageConfig storageConfig) {
    String tokenValue = storageConfig.getJsonKeyFilePath();
    if (tokenValue == null || tokenValue.isEmpty()) {
      tokenValue = String.format("testing://%s", storageConfig.getBucketPath());
    }
    Instant expirationInstant = Instant.ofEpochMilli(TOKEN_EXPIRATION_MILLIS);
    this.staticToken =
        AccessToken.newBuilder()
            .setTokenValue(tokenValue)
            .setExpirationTime(Date.from(expirationInstant))
            .build();
  }

  @Override
  public AccessToken generate(CredentialContext context) {
    return staticToken;
  }
}
