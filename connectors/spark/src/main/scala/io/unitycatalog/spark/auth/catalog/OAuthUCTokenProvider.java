package io.unitycatalog.spark.auth.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.spark.ApiClientConf;
import io.unitycatalog.spark.RetryingApiClient;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.utils.Clock;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import org.sparkproject.guava.base.Preconditions;

/**
 * OAuth-based token provider that fetches and automatically renews access tokens.
 *
 * <p>This provider uses the OAuth 2.0 client credentials flow to obtain access tokens.
 * It automatically renews tokens before they expire (default: 30 seconds before expiration).
 * Token requests are retried using {@link RetryingApiClient} for resilience against
 * transient network failures.</p>
 */
public class OAuthUCTokenProvider implements UCTokenProvider {
  private static final long DEFAULT_LEAD_RENEWAL_TIME_SECONDS = 30L;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String oauthUri;
  private final String oauthClientId;
  private final String oauthClientSecret;
  private final long leadRenewalTimeSeconds;
  private final HttpClient httpClient;
  private final Clock clock;

  private volatile TempToken tempToken;

  public OAuthUCTokenProvider(String oauthUri, String oauthClientId, String oauthClientSecret) {
    this(oauthUri, oauthClientId, oauthClientSecret, DEFAULT_LEAD_RENEWAL_TIME_SECONDS,
        new RetryingApiClient(new ApiClientConf(), Clock.systemClock()), Clock.systemClock());
  }

  // Package-private constructor for testing with custom dependencies
  OAuthUCTokenProvider(
      String oauthUri,
      String oauthClientId,
      String oauthClientSecret,
      long leadRenewalTimeSeconds,
      RetryingApiClient retryingApiClient,
      Clock clock) {
    Preconditions.checkNotNull(oauthUri, "OAuth URI must not be null");
    Preconditions.checkNotNull(oauthClientId, "OAuth client ID must not be null");
    Preconditions.checkNotNull(oauthClientSecret, "OAuth client secret must not be null");
    Preconditions.checkArgument(leadRenewalTimeSeconds >= 0,
        "Lead renewal time must be non-negative, but got %s", leadRenewalTimeSeconds);
    Preconditions.checkNotNull(retryingApiClient, "Retrying API client must not be null");
    Preconditions.checkNotNull(clock, "Clock must not be null");

    this.oauthUri = oauthUri;
    this.oauthClientId = oauthClientId;
    this.oauthClientSecret = oauthClientSecret;
    this.leadRenewalTimeSeconds = leadRenewalTimeSeconds;
    this.httpClient = retryingApiClient.getHttpClient();
    this.clock = clock;
  }

  @Override
  public String accessToken() {
    if (tempToken == null || tempToken.isReadyToRenew()) {
      synchronized (this) {
        if (tempToken == null || tempToken.isReadyToRenew()) {
          tempToken = renewToken();
        }
      }
    }
    return tempToken.token();
  }

  @Override
  public Map<String, String> properties() {
    return Map.of(
        UCHadoopConf.UC_OAUTH_URI, oauthUri,
        UCHadoopConf.UC_OAUTH_CLIENT_ID, oauthClientId,
        UCHadoopConf.UC_OAUTH_CLIENT_SECRET, oauthClientSecret);
  }

  private TempToken renewToken() {
    try {
      // Prepare Basic authentication header
      String credentials = String.format("%s:%s", oauthClientId, oauthClientSecret);
      String encodedCredentials = Base64.getEncoder()
          .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

      // Prepare form data
      String formData = "grant_type=client_credentials&scope=all-apis";

      // Build HTTP request
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(oauthUri))
          .header("Authorization", "Basic " + encodedCredentials)
          .header("Content-Type", "application/x-www-form-urlencoded")
          .POST(HttpRequest.BodyPublishers.ofString(formData))
          .build();

      // Send request with retry support
      HttpResponse<String> response = httpClient.send(request,
          HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        throw new IOException(String.format(
            "Failed to fetch OAuth token. HTTP status: %d, Response: %s",
            response.statusCode(), response.body()));
      }

      // Parse JSON response
      JsonNode jsonNode = OBJECT_MAPPER.readTree(response.body());
      String accessToken = jsonNode.get("access_token").asText();
      long expiresInSeconds = jsonNode.get("expires_in").asLong();

      return new TempToken(accessToken, clock.now().plusSeconds(expiresInSeconds));
    } catch (Exception e) {
      throw new RuntimeException("Failed to renew OAuth token", e);
    }
  }

  private class TempToken {
    private final String token;
    private final Instant expirationTime;

    TempToken(String token, Instant expirationTime) {
      this.token = token;
      this.expirationTime = expirationTime;
    }

    public String token() {
      return token;
    }

    public boolean isReadyToRenew() {
      Instant renewalTime = expirationTime.minusSeconds(leadRenewalTimeSeconds);
      return clock.now().isAfter(renewalTime);
    }
  }
}
