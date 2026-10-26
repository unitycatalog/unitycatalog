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
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
 * derived from {@code catalogUri}, and the UC-issued token is cached and renewed instead. This is
 * required by Unity Catalog servers that only accept their own tokens on API calls.
 *
 * <p>A token is cached until it is about to expire. The expiry comes from the response's {@code
 * expires_in}, which OAuth makes optional, or else from the token's own {@code exp} claim when it
 * is a JWT. A token with neither is used for one request only and not cached.
 */
class OAuthTokenProvider implements TokenProvider {
  private static final long DEFAULT_LEAD_RENEWAL_TIME_SECONDS = 30L;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final System.Logger LOGGER = System.getLogger(OAuthTokenProvider.class.getName());
  private static final String TOKEN_EXCHANGE_PATH = "/api/1.0/unity-control/auth/tokens";
  private static final String TOKEN_EXCHANGE_GRANT_TYPE =
      "urn:ietf:params:oauth:grant-type:token-exchange";
  private static final String ACCESS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token";

  private String oauthUri;
  private String oauthClientId;
  private String oauthClientSecret;
  // Empty when the request carries no scope.
  private String oauthScope;
  // Both set only when the OAuth token is exchanged for a UC token.
  private String catalogUri;
  private URI tokenExchangeUri;
  private long leadRenewalTimeSeconds;
  private HttpClient httpClient;
  private Clock clock;

  private volatile TempToken tempToken;
  private volatile boolean hasWarnedUncacheableToken;

  OAuthTokenProvider() {}

  // Package-private constructor for testing; a non-null catalogUri enables the UC token exchange.
  OAuthTokenProvider(
      String oauthUri,
      String oauthClientId,
      String oauthClientSecret,
      String oauthScope,
      String catalogUri,
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
    this.catalogUri = catalogUri;
    this.tokenExchangeUri = catalogUri == null ? null : tokenExchangeUri(catalogUri);
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
      String catalogUri = configs.get(AuthConfigs.CATALOG_URI);
      Preconditions.checkArgument(
          catalogUri != null && !catalogUri.isEmpty(),
          "Configuration key '%s' is missing or empty, it is required when '%s' is true",
          AuthConfigs.CATALOG_URI,
          AuthConfigs.OAUTH_EXCHANGE_FOR_UC_TOKEN);
      this.catalogUri = catalogUri;
      this.tokenExchangeUri = tokenExchangeUri(catalogUri);
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
    Map<String, String> configs = new HashMap<>();
    configs.put(AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE_VALUE);
    configs.put(AuthConfigs.OAUTH_URI, oauthUri);
    configs.put(AuthConfigs.OAUTH_CLIENT_ID, oauthClientId);
    configs.put(AuthConfigs.OAUTH_CLIENT_SECRET, oauthClientSecret);
    configs.put(AuthConfigs.OAUTH_SCOPE, oauthScope);
    if (shouldExchangeForUcToken()) {
      configs.put(AuthConfigs.OAUTH_EXCHANGE_FOR_UC_TOKEN, "true");
      configs.put(AuthConfigs.CATALOG_URI, catalogUri);
    }
    return Collections.unmodifiableMap(configs);
  }

  private TempToken renewToken() {
    try {
      JsonNode oauthResponse = fetchOAuthToken();
      if (!shouldExchangeForUcToken()) {
        return tempTokenOf(oauthResponse);
      }
      // The OAuth token is used once, to get the UC token, so only the UC token's expiry matters.
      return tempTokenOf(exchangeOAuthToken(accessTokenOf(oauthResponse)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to renew OAuth token", e);
    }
  }

  private boolean shouldExchangeForUcToken() {
    return tokenExchangeUri != null;
  }

  private JsonNode fetchOAuthToken() throws IOException, InterruptedException {
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
  private JsonNode exchangeOAuthToken(String subjectToken)
      throws IOException, InterruptedException {
    String formData =
        "grant_type="
            + URLEncoder.encode(TOKEN_EXCHANGE_GRANT_TYPE, StandardCharsets.UTF_8)
            + "&requested_token_type="
            + URLEncoder.encode(ACCESS_TOKEN_TYPE, StandardCharsets.UTF_8)
            + "&subject_token_type="
            + URLEncoder.encode(ACCESS_TOKEN_TYPE, StandardCharsets.UTF_8)
            + "&subject_token="
            + URLEncoder.encode(subjectToken, StandardCharsets.UTF_8);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(tokenExchangeUri)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(formData))
            .build();
    return sendTokenRequest(request, "Failed to exchange OAuth token for a UC token");
  }

  /**
   * Appends the token exchange path to the catalog URI's path, keeping any path prefix. Like {@code
   * ApiClientBuilder}, only the scheme, authority and path are used; a query or fragment is
   * dropped.
   *
   * @throws IllegalArgumentException if the catalog URI is not an http or https URI with a host
   */
  static URI tokenExchangeUri(String catalogUri) {
    URI uri;
    try {
      uri = new URI(catalogUri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format(
              "Configuration key '%s' is not a valid URI: '%s'",
              AuthConfigs.CATALOG_URI, catalogUri),
          e);
    }
    Preconditions.checkArgument(
        ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
            && uri.getHost() != null,
        "Configuration key '%s' must be an http or https URI with a host, but got '%s'",
        AuthConfigs.CATALOG_URI,
        catalogUri);
    String path = uri.getRawPath() == null ? "" : uri.getRawPath().replaceAll("/+$", "");
    return URI.create(uri.getScheme() + "://" + uri.getRawAuthority() + path + TOKEN_EXCHANGE_PATH);
  }

  private JsonNode sendTokenRequest(HttpRequest request, String errorMessage)
      throws IOException, InterruptedException {
    // Send request with retry support
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException(
          String.format(
              "%s. HTTP status: %d, Response: %s",
              errorMessage, response.statusCode(), response.body()));
    }

    return OBJECT_MAPPER.readTree(response.body());
  }

  private static String accessTokenOf(JsonNode response) throws IOException {
    JsonNode accessToken = response.get("access_token");
    if (accessToken == null || !accessToken.isTextual() || accessToken.asText().isEmpty()) {
      throw new IOException("Token response has no 'access_token'");
    }
    return accessToken.asText();
  }

  /**
   * Caches the token until {@code expires_in}, else until its JWT {@code exp} claim. A token with
   * neither is used once: it gets no expiry, so the next request fetches a new one.
   */
  private TempToken tempTokenOf(JsonNode response) throws IOException {
    String accessToken = accessTokenOf(response);
    // asLong also parses a numeric string, which some identity providers send.
    JsonNode expiresIn = response.get("expires_in");
    long expiresInSeconds = expiresIn == null ? -1L : expiresIn.asLong(-1L);
    if (expiresInSeconds >= 0) {
      return new TempToken(accessToken, clock.now().plusSeconds(expiresInSeconds));
    }
    Optional<Instant> jwtExpiration = jwtExpirationOf(accessToken);
    if (jwtExpiration.isPresent()) {
      return new TempToken(accessToken, jwtExpiration.get());
    }
    if (!hasWarnedUncacheableToken) {
      hasWarnedUncacheableToken = true;
      LOGGER.log(
          System.Logger.Level.WARNING,
          "Token response has no 'expires_in' and the token has no 'exp' claim, so it cannot be"
              + " cached: a new token is fetched for every request.");
    }
    return new TempToken(accessToken, /* expirationTime= */ null);
  }

  /**
   * The {@code exp} claim of a JWT, or empty if the token is not a JWT or has none. The signature
   * is not checked: this only decides how long to cache the token, and the server verifies it.
   */
  static Optional<Instant> jwtExpirationOf(String token) {
    String[] parts = token.split("\\.");
    if (parts.length != 3) {
      return Optional.empty();
    }
    try {
      JsonNode exp = OBJECT_MAPPER.readTree(Base64.getUrlDecoder().decode(parts[1])).get("exp");
      return exp != null && exp.canConvertToLong()
          ? Optional.of(Instant.ofEpochSecond(exp.asLong()))
          : Optional.empty();
    } catch (IllegalArgumentException | IOException e) {
      return Optional.empty();
    }
  }

  private static String normalizeOAuthScope(String oauthScope) {
    if (oauthScope == null) {
      return AuthConfigs.DEFAULT_OAUTH_SCOPE;
    }

    return oauthScope.trim().replaceAll("\\s+", " ");
  }

  private class TempToken {
    private final String token;
    // Null for a token that cannot be cached.
    private final Instant expirationTime;

    TempToken(String token, Instant expirationTime) {
      this.token = token;
      this.expirationTime = expirationTime;
    }

    public String token() {
      return token;
    }

    public boolean isReadyToRenew() {
      if (expirationTime == null) {
        return true;
      }
      Instant renewalTime = expirationTime.minusSeconds(leadRenewalTimeSeconds);
      return clock.now().isAfter(renewalTime);
    }
  }
}
