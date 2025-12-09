package io.unitycatalog.client.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.internal.RetryingApiClient;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

/**
 * OAuth-based token provider that fetches and automatically renews access tokens.
 *
 * <p>This provider uses the OAuth 2.0 client credentials flow to obtain access tokens. It
 * automatically renews tokens before they expire (default: 30 seconds before expiration). Token
 * requests are retried automatically for resilience against transient network failures.
 */
class OAuthTokenProvider implements TokenProvider {
  private static final long DEFAULT_LEAD_RENEWAL_TIME_SECONDS = 30L;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String oauthUri;
  private String oauthClientId;
  private String oauthClientSecret;
  private long leadRenewalTimeSeconds;
  private HttpClient httpClient;
  private Clock clock;

  private volatile TempToken tempToken;

  OAuthTokenProvider() {}

  // Package-private constructor for testing with custom dependencies
  OAuthTokenProvider(
      String oauthUri,
      String oauthClientId,
      String oauthClientSecret,
      long leadRenewalTimeSeconds,
      ApiClient apiClient,
      Clock clock) {
    Preconditions.checkNotNull(oauthUri, "OAuth URI must not be null");
    Preconditions.checkNotNull(oauthClientId, "OAuth client ID must not be null");
    Preconditions.checkNotNull(oauthClientSecret, "OAuth client secret must not be null");
    Preconditions.checkArgument(
        leadRenewalTimeSeconds >= 0,
        "Lead renewal time must be non-negative, but got %s",
        leadRenewalTimeSeconds);
    Preconditions.checkNotNull(apiClient, "API client must not be null");
    Preconditions.checkNotNull(clock, "Clock must not be null");

    this.oauthUri = oauthUri;
    this.oauthClientId = oauthClientId;
    this.oauthClientSecret = oauthClientSecret;
    this.leadRenewalTimeSeconds = leadRenewalTimeSeconds;
    this.httpClient = apiClient.getHttpClient();
    this.clock = clock;
  }

  @Override
  public void initialize(Map<String, String> configs) {
    // Parse and validate the Oauth URI.
    String oauthUri = configs.get(AuthProps.OAUTH_URI);
    Preconditions.checkArgument(
        oauthUri != null && !oauthUri.isEmpty(), "OAuth URI must not be null or empty");
    this.oauthUri = oauthUri;

    // Parse and validate the OAuth Client ID.
    String oauthClientId = configs.get(AuthProps.OAUTH_CLIENT_ID);
    Preconditions.checkArgument(
        oauthClientId != null && !oauthClientId.isEmpty(),
        "OAuth Client ID must not be null or empty");
    this.oauthClientId = oauthClientId;

    // Parse and validate the OAuth Client Secret.
    String oauthClientSecret = configs.get(AuthProps.OAUTH_CLIENT_SECRET);
    Preconditions.checkArgument(
        oauthClientSecret != null && !oauthClientSecret.isEmpty(),
        "OAuth Client Secret must not be null or empty");
    this.oauthClientSecret = oauthClientSecret;

    this.leadRenewalTimeSeconds = DEFAULT_LEAD_RENEWAL_TIME_SECONDS;
    this.httpClient =
        new RetryingApiClient(JitterDelayRetryPolicy.builder().build()).getHttpClient();
    this.clock = Clock.systemClock();
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
  public Map<String, String> getConfigs() {
    return Map.of(
        AuthProps.AUTH_TYPE, AuthProps.OAUTH_AUTH_TYPE,
        AuthProps.OAUTH_URI, oauthUri,
        AuthProps.OAUTH_CLIENT_ID, oauthClientId,
        AuthProps.OAUTH_CLIENT_SECRET, oauthClientSecret);
  }

  private TempToken renewToken() {
    try {
      // Prepare Basic authentication header
      String credentials = String.format("%s:%s", oauthClientId, oauthClientSecret);
      String encodedCredentials =
          Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

      // Prepare form data
      String formData = "grant_type=client_credentials&scope=all-apis";

      // Build HTTP request
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(oauthUri))
              .header("Authorization", "Basic " + encodedCredentials)
              .header("Content-Type", "application/x-www-form-urlencoded")
              .POST(HttpRequest.BodyPublishers.ofString(formData))
              .build();

      // Send request with retry support
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        throw new IOException(
            String.format(
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
