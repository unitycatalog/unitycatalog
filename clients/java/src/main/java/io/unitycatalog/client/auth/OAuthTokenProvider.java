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
 * <p>The token request carries {@code oauth.scope}, or {@code all-apis} when it is unset. An empty
 * {@code oauth.scope} sends no scope, which OAuth allows (RFC 6749 section 4.4), so the identity
 * provider applies its own default.
 *
 * <p>When {@code oauth.exchangeForUcToken} is {@code true}, the OAuth token is not sent to Unity
 * Catalog directly. It is exchanged (RFC 8693) at the server's {@code auth/tokens} endpoint,
 * derived from the catalog {@code uri}, and the UC-issued token is cached and renewed instead. This
 * is required by Unity Catalog servers that only accept their own tokens on API calls.
 */
class OAuthTokenProvider implements TokenProvider {
  private static final long DEFAULT_LEAD_RENEWAL_TIME_SECONDS = 30L;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String TOKEN_EXCHANGE_PATH = "/api/1.0/unity-control/auth/tokens";
  private static final String ACCESS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token";

  private String oauthUri;
  private String oauthClientId;
  private String oauthClientSecret;
  // Empty when the request carries no scope.
  private String oauthScope;
  // Set only when the OAuth token is exchanged for a UC token.
  private String ucUri;
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
      long leadRenewalTimeSeconds,
      ApiClient apiClient,
      Clock clock) {
    this(
        oauthUri,
        oauthClientId,
        oauthClientSecret,
        oauthScope,
        /* ucUri= */ null,
        leadRenewalTimeSeconds,
        apiClient,
        clock);
  }

  // Package-private constructor for testing; a non-null ucUri enables the UC token exchange.
  OAuthTokenProvider(
      String oauthUri,
      String oauthClientId,
      String oauthClientSecret,
      String oauthScope,
      String ucUri,
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
    this.ucUri = ucUri;
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

    // Optional OAuth scope; defaults to all-apis when unset, and is left out when empty.
    this.oauthScope = normalizeOAuthScope(configs.get(AuthConfigs.OAUTH_SCOPE));

    // Optional exchange of the OAuth token for a UC token; needs the catalog URI to reach UC.
    if (Boolean.parseBoolean(configs.get(AuthConfigs.OAUTH_EXCHANGE_FOR_UC_TOKEN))) {
      String ucUri = configs.get(AuthConfigs.UC_URI);
      Preconditions.checkArgument(
          ucUri != null && !ucUri.isEmpty(),
          "Configuration key '%s' is missing or empty, it is required when '%s' is true",
          AuthConfigs.UC_URI,
          AuthConfigs.OAUTH_EXCHANGE_FOR_UC_TOKEN);
      this.ucUri = ucUri;
    }

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
    if (!shouldExchangeForUcToken()) {
      return Map.of(
          AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE_VALUE,
          AuthConfigs.OAUTH_URI, oauthUri,
          AuthConfigs.OAUTH_CLIENT_ID, oauthClientId,
          AuthConfigs.OAUTH_CLIENT_SECRET, oauthClientSecret,
          AuthConfigs.OAUTH_SCOPE, oauthScope);
    }
    return Map.of(
        AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE_VALUE,
        AuthConfigs.OAUTH_URI, oauthUri,
        AuthConfigs.OAUTH_CLIENT_ID, oauthClientId,
        AuthConfigs.OAUTH_CLIENT_SECRET, oauthClientSecret,
        AuthConfigs.OAUTH_SCOPE, oauthScope,
        AuthConfigs.OAUTH_EXCHANGE_FOR_UC_TOKEN, "true",
        AuthConfigs.UC_URI, ucUri);
  }

  private TempToken renewToken() {
    try {
      TempToken oauthToken = fetchOAuthToken();
      return shouldExchangeForUcToken() ? exchangeOAuthToken(oauthToken.token()) : oauthToken;
    } catch (Exception e) {
      throw new RuntimeException("Failed to renew OAuth token", e);
    }
  }

  private boolean shouldExchangeForUcToken() {
    return ucUri != null;
  }

  private TempToken fetchOAuthToken() throws IOException, InterruptedException {
    // Prepare Basic authentication header
    String credentials = String.format("%s:%s", oauthClientId, oauthClientSecret);
    String encodedCredentials =
        Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

    // Prepare form data
    String formData = "grant_type=client_credentials";
    if (!oauthScope.isEmpty()) {
      formData += "&scope=" + URLEncoder.encode(oauthScope, StandardCharsets.UTF_8);
    }

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(oauthUri))
            .header("Authorization", "Basic " + encodedCredentials)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(formData))
            .build();
    return sendTokenRequest(request, "Failed to fetch OAuth token");
  }

  /** Exchanges the OAuth token for a UC-issued token at the UC server (RFC 8693). */
  private TempToken exchangeOAuthToken(String subjectToken)
      throws IOException, InterruptedException {
    String formData =
        "grant_type="
            + URLEncoder.encode(
                "urn:ietf:params:oauth:grant-type:token-exchange", StandardCharsets.UTF_8)
            + "&requested_token_type="
            + URLEncoder.encode(ACCESS_TOKEN_TYPE, StandardCharsets.UTF_8)
            + "&subject_token_type="
            + URLEncoder.encode(ACCESS_TOKEN_TYPE, StandardCharsets.UTF_8)
            + "&subject_token="
            + URLEncoder.encode(subjectToken, StandardCharsets.UTF_8);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(tokenExchangeUri(ucUri))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(formData))
            .build();
    return sendTokenRequest(request, "Failed to exchange OAuth token for a UC token");
  }

  /** Appends the token exchange path to the catalog URI, keeping any path prefix it has. */
  static URI tokenExchangeUri(String ucUri) {
    return URI.create(ucUri.replaceAll("/+$", "") + TOKEN_EXCHANGE_PATH);
  }

  private TempToken sendTokenRequest(HttpRequest request, String errorMessage)
      throws IOException, InterruptedException {
    // Send request with retry support
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException(
          String.format(
              "%s. HTTP status: %d, Response: %s",
              errorMessage, response.statusCode(), response.body()));
    }

    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.body());
    String accessToken = jsonNode.get("access_token").asText();
    long expiresInSeconds = jsonNode.get("expires_in").asLong();

    return new TempToken(accessToken, clock.now().plusSeconds(expiresInSeconds));
  }

  private static String normalizeOAuthScope(String oauthScope) {
    if (oauthScope == null) {
      return AuthConfigs.DEFAULT_OAUTH_SCOPE;
    }

    return oauthScope.trim().replaceAll("\\s+", " ");
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
