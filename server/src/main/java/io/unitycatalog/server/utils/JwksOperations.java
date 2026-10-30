package io.unitycatalog.server.utils;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.JwkProviderBuilder;
import com.auth0.jwk.JwksHttpClient;
import com.auth0.jwk.JwksHttpResponse;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.linecorp.armeria.client.ResponseTimeoutMode;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.client.redirect.RedirectConfig;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.OAuthInvalidClientException;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import io.unitycatalog.server.security.SecurityContext;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Loads signing keys from trusted identity providers and reuses providers across requests. */
public class JwksOperations {

  private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(5);
  private static final int MAX_RESPONSE_LENGTH = 1024 * 1024;
  private static final int MAX_PROVIDERS = 100;
  private static final int MAX_CACHED_KEYS = 100;
  private static final Duration CACHE_TTL = Duration.ofHours(1);
  private static final Duration DISCOVERY_FAILURE_TTL = Duration.ofSeconds(30);
  private static final Duration MIN_KEY_REFRESH_INTERVAL = Duration.ofSeconds(6);

  private final WebClient webClient;
  private static final ObjectMapper mapper = new ObjectMapper();
  // Checks the server's own tokens with its signing key. Tests of external issuers pass no context.
  private final JWTVerifier internalVerifier;
  private final Ticker ticker;
  private final Set<URI> allowedJwksUris;
  private final Cache<String, JwkProvider> providers;
  private final Cache<String, DiscoveryFailure> discoveryFailures;

  private static final Logger LOGGER = LoggerFactory.getLogger(JwksOperations.class);
  private static final int MAX_LOGGED_BODY_LENGTH = 500;

  public JwksOperations(SecurityContext securityContext) {
    this(securityContext, List.of());
  }

  /**
   * @param securityContext internal signing-key configuration
   * @param allowedJwksUris exact URLs permitted in addition to the issuer's own origin
   */
  public JwksOperations(SecurityContext securityContext, List<String> allowedJwksUris) {
    this(securityContext, allowedJwksUris, Ticker.systemTicker(), RESPONSE_TIMEOUT);
  }

  JwksOperations(
      SecurityContext securityContext,
      List<String> allowedJwksUris,
      Ticker ticker,
      Duration responseTimeout) {
    this.internalVerifier =
        securityContext == null
            ? null
            : JWT.require(securityContext.getAlgorithm()).withIssuer(INTERNAL).build();
    this.ticker = ticker;
    this.webClient =
        WebClient.builder()
            .followRedirects(RedirectConfig.disabled())
            .responseTimeoutMode(ResponseTimeoutMode.FROM_START)
            .responseTimeout(responseTimeout)
            .maxResponseLength(MAX_RESPONSE_LENGTH)
            .build();
    this.allowedJwksUris =
        Set.copyOf(allowedJwksUris.stream().map(JwksOperations::configuredJwksUri).toList());
    // Bound wildcard-issuer cardinality and the lifetime of both discovery metadata and keys.
    // Run cache maintenance inline so these small caches do not use the common pool.
    this.providers =
        Caffeine.newBuilder()
            .maximumSize(MAX_PROVIDERS)
            .expireAfterWrite(CACHE_TTL)
            .ticker(ticker)
            .executor(Runnable::run)
            .build();
    this.discoveryFailures =
        Caffeine.newBuilder()
            .maximumSize(MAX_PROVIDERS)
            .expireAfterWrite(DISCOVERY_FAILURE_TTL)
            .ticker(ticker)
            .executor(Runnable::run)
            .build();
  }

  /**
   * Returns the verifier for a token from the given issuer. For the server's own ({@code internal})
   * tokens, {@code keyId} and {@code alg} are ignored.
   */
  @SneakyThrows
  public JWTVerifier verifierForIssuerAndKey(String issuer, String keyId, String alg) {
    if (issuer.equals(INTERNAL)) {
      // The key ID of the server's own tokens is ignored.
      return internalVerifier;
    }
    JwkProvider jwkProvider = loadJwkProvider(issuer);
    Jwk jwk;
    try {
      jwk = jwkProvider.get(keyId);
    } catch (JwkException e) {
      LOGGER.warn("Failed to get signing key '{}' for issuer '{}'", keyId, issuer, e);
      throw new OAuthInvalidRequestException(
          ErrorCode.INTERNAL,
          String.format("Could not get signing key '%s' for issuer %s", keyId, issuer),
          e);
    }

    Algorithm algorithm = algorithmForJwk(jwk, alg);

    return JWT.require(algorithm).withIssuer(issuer).build();
  }

  @SneakyThrows
  private Algorithm algorithmForJwk(Jwk jwk, String alg) {
    String keyType = jwk.getType();

    return switch (keyType) {
      case "RSA" ->
          switch (alg) {
            case "RS256" -> Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), null);
            case "RS384" -> Algorithm.RSA384((RSAPublicKey) jwk.getPublicKey(), null);
            case "RS512" -> Algorithm.RSA512((RSAPublicKey) jwk.getPublicKey(), null);
            default ->
                throw new OAuthInvalidClientException(
                    ErrorCode.ABORTED, String.format("Unsupported RSA algorithm: %s", alg));
          };
      case "EC" ->
          switch (alg) {
            case "ES256" -> Algorithm.ECDSA256((ECPublicKey) jwk.getPublicKey(), null);
            case "ES384" -> Algorithm.ECDSA384((ECPublicKey) jwk.getPublicKey(), null);
            case "ES512" -> Algorithm.ECDSA512((ECPublicKey) jwk.getPublicKey(), null);
            default ->
                throw new OAuthInvalidClientException(
                    ErrorCode.ABORTED, String.format("Unsupported ECDSA algorithm: %s", alg));
          };
      default ->
          throw new OAuthInvalidClientException(
              ErrorCode.ABORTED, String.format("Unsupported key type: %s", keyType));
    };
  }

  /**
   * Reuses an external provider after the caller checks the issuer allowlist.
   *
   * @param issuer an external token issuer
   * @return the signing-key provider
   */
  @SneakyThrows
  public JwkProvider loadJwkProvider(String issuer) {
    LOGGER.debug("Loading JwkProvider for issuer '{}'", issuer);
    // Share discovery for each issuer. A slow load can also delay issuers in the same hash bin.
    return providers.get(issuer, this::fetchJwkProvider);
  }

  private JwkProvider fetchJwkProvider(String issuer) {
    DiscoveryFailure failure = discoveryFailures.getIfPresent(issuer);
    if (failure != null) {
      throw new OAuthInvalidRequestException(failure.errorCode(), failure.message());
    }
    String issuerUrl = issuer.contains("://") ? issuer : "https://" + issuer;
    String wellKnownConfigUrl = issuerUrl;
    if (!wellKnownConfigUrl.endsWith("/")) {
      wellKnownConfigUrl += "/";
    }
    var path = wellKnownConfigUrl + ".well-known/openid-configuration";
    try {
      // Get the JWKS from the OIDC well-known location described here
      // https://openid.net/specs/openid-connect-discovery-1_0-21.html#ProviderConfig

      URI issuerUri = httpUri(issuerUrl);
      if (issuerUri.getRawQuery() != null) {
        throw new OAuthInvalidRequestException(ErrorCode.ABORTED, "Invalid issuer URL");
      }

      LOGGER.debug("path: {}", path);

      AggregatedHttpResponse httpResponse = webClient.get(path).aggregate().join();
      String response = httpResponse.contentUtf8();

      // The body is only logged, not returned: callers of token exchange are not yet
      // authenticated, and a proxy error page may describe internal infrastructure.
      if (!httpResponse.status().isSuccess()) {
        LOGGER.warn(
            "Failed to fetch issuer configuration from '{}': status={}, body='{}'",
            path,
            httpResponse.status(),
            abbreviate(response));
        throw new OAuthInvalidRequestException(
            ErrorCode.INTERNAL,
            String.format(
                "Could not get issuer configuration from %s: HTTP %s",
                path, httpResponse.status()));
      }

      JsonNode config;
      try {
        config = mapper.readTree(response);
      } catch (JsonProcessingException e) {
        LOGGER.warn(
            "Issuer configuration from '{}' is not valid JSON: contentType={}, body='{}'",
            path,
            httpResponse.contentType(),
            abbreviate(response));
        throw new OAuthInvalidRequestException(
            ErrorCode.INTERNAL,
            String.format("Issuer configuration from %s is not valid JSON", path));
      }

      if (config.isEmpty()) {
        throw new OAuthInvalidRequestException(
            ErrorCode.ABORTED, "Could not get issuer configuration from " + path);
      }

      String configIssuer = config.path("issuer").textValue();
      String configJwksUri = config.path("jwks_uri").textValue();

      if (!issuerUrl.equals(configIssuer)) {
        throw new OAuthInvalidRequestException(
            ErrorCode.ABORTED,
            String.format(
                "Issuer '%s' doesn't match configuration issuer '%s' from %s",
                issuerUrl, configIssuer, path));
      }

      if (configJwksUri == null) {
        throw new OAuthInvalidRequestException(ErrorCode.ABORTED, "JWKS configuration missing");
      }

      URI jwksUri = jwksUriForIssuer(issuerUri, configJwksUri);

      return new JwkProviderBuilder(jwksUri.toURL())
          .cached(MAX_CACHED_KEYS, CACHE_TTL)
          .rateLimited(false)
          .httpClient(rateLimitedSigningKeysClient())
          .build();
    } catch (OAuthInvalidRequestException e) {
      // Failed loads also need a cooldown; the provider cache only stores successes.
      discoveryFailures.put(issuer, new DiscoveryFailure(e.getErrorCode(), e.getMessage()));
      throw e;
    } catch (Exception e) {
      String message = "Could not get issuer configuration from " + path;
      discoveryFailures.put(issuer, new DiscoveryFailure(ErrorCode.INTERNAL, message));
      throw new OAuthInvalidRequestException(ErrorCode.INTERNAL, message, e);
    }
  }

  private record DiscoveryFailure(ErrorCode errorCode, String message) {}

  /** Limits HTTP fetches per provider while keeping its full cached key set available. */
  private JwksHttpClient rateLimitedSigningKeysClient() {
    return new JwksHttpClient() {
      private long nextRefreshNanos = ticker.read();

      @Override
      public synchronized JwksHttpResponse fetch(URL url) throws IOException {
        long now = ticker.read();
        if (now - nextRefreshNanos < 0) {
          throw new IOException("Signing-key refresh is rate limited");
        }
        nextRefreshNanos = now + MIN_KEY_REFRESH_INTERVAL.toNanos();
        return fetchSigningKeys(url);
      }
    };
  }

  private JwksHttpResponse fetchSigningKeys(URL url) throws IOException {
    try {
      AggregatedHttpResponse keys = webClient.get(url.toString()).aggregate().join();
      if (!keys.status().isSuccess()) {
        throw new IOException("Could not get signing keys: HTTP " + keys.status());
      }
      return new JwksHttpResponse(keys.contentUtf8());
    } catch (CompletionException e) {
      throw new IOException("Could not get signing keys", e);
    }
  }

  /**
   * Accepts the issuer's origin or an exact configured URL, with no HTTPS downgrade.
   *
   * @param issuerUri validated discovery issuer URL
   * @param value discovery document's JWKS URL
   * @return the validated JWKS URL
   */
  URI jwksUriForIssuer(URI issuerUri, String value) {
    URI jwksUri = httpUri(value);
    boolean isSameOrigin =
        issuerUri.getScheme().equalsIgnoreCase(jwksUri.getScheme())
            && issuerUri.getHost().equalsIgnoreCase(jwksUri.getHost())
            && effectivePort(issuerUri) == effectivePort(jwksUri);
    boolean isDowngrade =
        issuerUri.getScheme().equalsIgnoreCase("https")
            && !jwksUri.getScheme().equalsIgnoreCase("https");
    if (isDowngrade || (!isSameOrigin && !allowedJwksUris.contains(jwksUri))) {
      LOGGER.warn(
          "Rejected JWKS URI '{}': HTTPS issuers require HTTPS keys; other origins must be listed"
              + " in {}",
          jwksUri,
          ServerProperties.Property.ALLOWED_JWKS_URIS.getKey());
      throw new OAuthInvalidRequestException(ErrorCode.ABORTED, "JWKS URI is not allowed");
    }

    return jwksUri;
  }

  private static URI configuredJwksUri(String value) {
    try {
      return httpUri(value);
    } catch (OAuthInvalidRequestException e) {
      throw new IllegalArgumentException(
          "Invalid " + ServerProperties.Property.ALLOWED_JWKS_URIS.getKey() + " entry: " + value,
          e);
    }
  }

  /** Rejects URL components that can change the destination or are not sent to the provider. */
  private static URI httpUri(String value) {
    URI uri;
    try {
      uri = URI.create(value);
    } catch (IllegalArgumentException e) {
      throw new OAuthInvalidRequestException(ErrorCode.ABORTED, "Invalid identity provider URL", e);
    }
    if (!("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
        || uri.getHost() == null
        || uri.getRawUserInfo() != null
        || uri.getRawFragment() != null
        || uri.getPort() == 0
        || uri.getPort() > 65535) {
      throw new OAuthInvalidRequestException(ErrorCode.ABORTED, "Invalid identity provider URL");
    }
    return uri;
  }

  private static int effectivePort(URI uri) {
    return uri.getPort() == -1
        ? (uri.getScheme().equalsIgnoreCase("https") ? 443 : 80)
        : uri.getPort();
  }

  private static String abbreviate(String body) {
    return StringUtils.abbreviate(body, MAX_LOGGED_BODY_LENGTH);
  }
}
