package io.unitycatalog.server.utils;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.JwkProviderBuilder;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.OAuthInvalidClientException;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import io.unitycatalog.server.security.SecurityContext;
import java.net.URI;
import java.nio.file.Path;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwksOperations {

  private final WebClient webClient = WebClient.builder().build();
  private static final ObjectMapper mapper = new ObjectMapper();
  private final SecurityContext securityContext;

  private static final Logger LOGGER = LoggerFactory.getLogger(JwksOperations.class);
  private static final int MAX_LOGGED_BODY_LENGTH = 500;

  public JwksOperations(SecurityContext securityContext) {
    this.securityContext = securityContext;
  }

  @SneakyThrows
  public JWTVerifier verifierForIssuerAndKey(String issuer, String keyId, String alg) {
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

  @SneakyThrows
  public JwkProvider loadJwkProvider(String issuer) {
    LOGGER.debug("Loading JwkProvider for issuer '{}'", issuer);
    if (issuer.equals(INTERNAL)) {
      // Return our own "self-signed" provider, for easy mode.
      // TODO: This should be configurable
      Path certsFile = securityContext.getCertsFile();
      return new JwkProviderBuilder(certsFile.toUri().toURL()).cached(false).build();
    } else {
      // Get the JWKS from the OIDC well-known location described here
      // https://openid.net/specs/openid-connect-discovery-1_0-21.html#ProviderConfig

      if (!issuer.startsWith("https://") && !issuer.startsWith("http://")) {
        issuer = "https://" + issuer;
      }

      String wellKnownConfigUrl = issuer;

      if (!wellKnownConfigUrl.endsWith("/")) {
        wellKnownConfigUrl += "/";
      }

      var path = wellKnownConfigUrl + ".well-known/openid-configuration";
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

      // TODO: We should cache this. No need to fetch it each time.
      Map<String, Object> configMap;
      try {
        configMap = mapper.readValue(response, new TypeReference<>() {});
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

      if (configMap == null || configMap.isEmpty()) {
        throw new OAuthInvalidRequestException(
            ErrorCode.ABORTED, "Could not get issuer configuration from " + path);
      }

      String configIssuer = (String) configMap.get("issuer");
      String configJwksUri = (String) configMap.get("jwks_uri");

      if (!issuer.equals(configIssuer)) {
        throw new OAuthInvalidRequestException(
            ErrorCode.ABORTED,
            String.format(
                "Issuer '%s' doesn't match configuration issuer '%s' from %s",
                issuer, configIssuer, path));
      }

      if (configJwksUri == null) {
        throw new OAuthInvalidRequestException(ErrorCode.ABORTED, "JWKS configuration missing");
      }

      // TODO: Or maybe just cache the provider for reuse.
      return new JwkProviderBuilder(URI.create(configJwksUri).toURL()).cached(false).build();
    }
  }

  private static String abbreviate(String body) {
    return StringUtils.abbreviate(body, MAX_LOGGED_BODY_LENGTH);
  }
}
