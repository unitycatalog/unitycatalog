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
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Internal class - not intended for direct use.
 *
 * <p>OAuth-based token provider that fetches and automatically renews access tokens.
 *
 * <p>This provider uses the OAuth 2.0 client credentials flow to obtain access tokens. It
 * automatically renews tokens before they expire (default: 30 seconds before expiration). Token
 * requests are retried automatically for resilience against transient network failures.
 *
 * <p>When {@code oauth.exchangeUri} is configured, the obtained IdP token is exchanged for a UC
 * token at that URI. The UC token and its expiration are used for caching and renewal.
 */
class OAuthTokenProvider implements TokenProvider {
  private static final long DEFAULT_LEAD_RENEWAL_TIME_SECONDS = 30L;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String oauthUri;
  private String oauthExchangeUri;
  private String oauthClientId;
  private String oauthClientSecret;
  private String oauthScope;
  private long leadRenewalTimeSeconds;
  private HttpClient httpClient;
  private Clock clock;

  private volatile TempToken tempToken;

  OAuthTokenProvider() {}

  // Package-private constructor for testing with a custom OAuth scope.
  OAuthTokenProvider(
      String oauthUri,
      String oauthClientId,
      String oauthClientSecret,
      String oauthScope,
      String oauthExchangeUri,
      long leadRenewalTimeSeconds,
      ApiClient apiClient,
      Clock clock) {
    Preconditions.checkNotNull(oauthUri, "OAuth URI must not be null");
    Preconditions.checkNotNull(oauthClientId, "OAuth client ID must not be null");
    Preconditions.checkNotNull(oauthClientSecret, "OAuth client secret must not be null");
    Preconditions.checkNotNull(oauthScope, "OAuth scope must not be null");
    Preconditions.checkArgument(
        leadRenewalTimeSeconds >= 0,
        "Lead renewal time must be non-negative, but got %s",
        leadRenewalTimeSeconds);
    Preconditions.checkNotNull(apiClient, "API client must not be null");
    Preconditions.checkNotNull(clock, "Clock must not be null");

    this.oauthUri = oauthUri;
    this.oauthClientId = oauthClientId;
    this.oauthClientSecret = oauthClientSecret;
    this.oauthScope = normalizeOAuthScope(oauthScope);
    this.oauthExchangeUri = oauthExchangeUri;
    this.leadRenewalTimeSeconds = leadRenewalTimeSeconds;
    this.httpClient = apiClient.getHttpClient();
    this.clock = clock;
  }

  @Override
  public void initialize(Map<String, String> configs) {
    // Parse and validate the OAuth URI.
    String oauthUri = configs.get(AuthConfigs.OAUTH_URI);
    Preconditions.checkArgument(
        oauthUri != null && !oauthUri.isEmpty(),
        "Configuration key '%s' is missing or empty",
        AuthConfigs.OAUTH_URI);
    this.oauthUri = oauthUri;

    // Parse and validate the OAuth Client ID.
    String oauthClientId = configs.get(AuthConfigs.OAUTH_CLIENT_ID);
    Preconditions.checkArgument(
        oauthClientId != null && !oauthClientId.isEmpty(),
        "Configuration key '%s' is missing or empty",
        AuthConfigs.OAUTH_CLIENT_ID);
    this.oauthClientId = oauthClientId;

    // Parse and validate the OAuth Client Secret.
    String oauthClientSecret = configs.get(AuthConfigs.OAUTH_CLIENT_SECRET);
    Preconditions.checkArgument(
        oauthClientSecret != null && !oauthClientSecret.isEmpty(),
        "Configuration key '%s' is missing or empty",
        AuthConfigs.OAUTH_CLIENT_SECRET);
    this.oauthClientSecret = oauthClientSecret;

    // Optional OAuth scope; defaults to all-apis when unset.
    this.oauthScope = normalizeOAuthScope(configs.get(AuthConfigs.OAUTH_SCOPE));

    this.oauthExchangeUri = configs.get(AuthConfigs.OAUTH_EXCHANGE_URI);
    Preconditions.checkArgument(
        oauthExchangeUri == null || !oauthExchangeUri.isBlank(),
        "Configuration key '%s' must not be empty",
        AuthConfigs.OAUTH_EXCHANGE_URI);

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
  public Map<String, String> configs() {
    Map<String, String> configs =
        new HashMap<>(
            Map.of(
                AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE_VALUE,
                AuthConfigs.OAUTH_URI, oauthUri,
                AuthConfigs.OAUTH_CLIENT_ID, oauthClientId,
                AuthConfigs.OAUTH_CLIENT_SECRET, oauthClientSecret,
                AuthConfigs.OAUTH_SCOPE, oauthScope));
    if (oauthExchangeUri != null) {
      configs.put(AuthConfigs.OAUTH_EXCHANGE_URI, oauthExchangeUri);
    }
    return Map.copyOf(configs);
  }

  private TempToken renewToken() {
    try {
      // Prepare Basic authentication header
      String credentials = String.format("%s:%s", oauthClientId, oauthClientSecret);
      String encodedCredentials =
          Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

      // Prepare form data
      String formData =
          "grant_type=client_credentials&scope="
              + URLEncoder.encode(oauthScope, StandardCharsets.UTF_8);

      // Build HTTP request
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(oauthUri))
              .header("Authorization", "Basic " + encodedCredentials)
              .header("Content-Type", "application/x-www-form-urlencoded")
              .POST(HttpRequest.BodyPublishers.ofString(formData))
              .build();

      // Get an IdP token using the client ID and client secret.
      JsonNode jsonNode = requestToken(request);
      if (oauthExchangeUri != null) {
        // Send the IdP token to UC's exchange endpoint to get a UC token.
        String tokenType =
            URLEncoder.encode(
                "urn:ietf:params:oauth:token-type:access_token", StandardCharsets.UTF_8);
        String exchangeFormData =
            "grant_type="
                + URLEncoder.encode(
                    "urn:ietf:params:oauth:grant-type:token-exchange", StandardCharsets.UTF_8)
                + "&requested_token_type="
                + tokenType
                + "&subject_token_type="
                + tokenType
                + "&subject_token="
                + URLEncoder.encode(jsonNode.get("access_token").asText(), StandardCharsets.UTF_8);
        HttpRequest exchangeRequest =
            HttpRequest.newBuilder()
                .uri(URI.create(oauthExchangeUri))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(exchangeFormData))
                .build();
        jsonNode = requestToken(exchangeRequest);
      }

      // Cache the UC token after exchange, or the IdP token when exchange is disabled.
      String accessToken = jsonNode.get("access_token").asText();
      long expiresInSeconds = jsonNode.get("expires_in").asLong();

      return new TempToken(accessToken, clock.now().plusSeconds(expiresInSeconds));
    } catch (Exception e) {
      throw new RuntimeException("Failed to renew OAuth token", e);
    }
  }

  private JsonNode requestToken(HttpRequest request) throws IOException, InterruptedException {
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new IOException(
          String.format(
              "Failed to fetch OAuth token. HTTP status: %d, Response: %s",
              response.statusCode(), response.body()));
    }
    return OBJECT_MAPPER.readTree(response.body());
  }

  private static String normalizeOAuthScope(String oauthScope) {
    if (oauthScope == null) {
      return AuthConfigs.DEFAULT_OAUTH_SCOPE;
    }

    String normalizedScope = oauthScope.trim().replaceAll("\\s+", " ");
    Preconditions.checkArgument(
        !normalizedScope.isEmpty(),
        "Configuration key '%s' must not be empty",
        AuthConfigs.OAUTH_SCOPE);
    return normalizedScope;
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
