package io.unitycatalog.server.utils;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.JwkProviderBuilder;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.OAuthInvalidClientException;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import io.unitycatalog.server.security.SecurityContext;
import lombok.SneakyThrows;

import java.net.URL;
import java.nio.file.Path;
import java.security.interfaces.RSAPublicKey;
import java.util.Map;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

public class JwksOperations {

  private final WebClient webClient = WebClient.builder().build();
  private static final ObjectMapper mapper = new ObjectMapper();

  public JwksOperations() {
  }

  @SneakyThrows
  public JWTVerifier verifierForIssuerAndKey(String issuer, String keyId) {
    JwkProvider jwkProvider = loadJwkProvider(issuer);
    Jwk jwk = jwkProvider.get(keyId);

    if (!"RSA".equalsIgnoreCase(jwk.getPublicKey().getAlgorithm())) {
      throw new OAuthInvalidRequestException(ErrorCode.ABORTED,
              String.format("Invalid algorithm '%s' for issuer '%s'",
                      jwk.getPublicKey().getAlgorithm(), issuer));
    }

    Algorithm algorithm = algorithmForJwk(jwk);

    return JWT.require(algorithm).withIssuer(issuer).build();
  }

  @SneakyThrows
  private Algorithm algorithmForJwk(Jwk jwk) {
    return switch (jwk.getAlgorithm()) {
      case "RS256" -> Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), null);
      case "RS384" -> Algorithm.RSA384((RSAPublicKey) jwk.getPublicKey(), null);
      case "RS512" -> Algorithm.RSA512((RSAPublicKey) jwk.getPublicKey(), null);
      default -> throw new OAuthInvalidClientException(ErrorCode.ABORTED,
              String.format("Unsupported algorithm: %s", jwk.getAlgorithm()));
    };
  }

  @SneakyThrows
  public JwkProvider loadJwkProvider(String issuer) {

    if (issuer.equals(INTERNAL)) {
      // Return our own "self-signed" provider, for easy mode.
      // TODO: This should be configurable
      return new JwkProviderBuilder(Path.of("etc/conf/certs.json").toUri().toURL()).cached(false).build();
    } else {
      // Get the JWKS from the OIDC well-known location described here
      // https://openid.net/specs/openid-connect-discovery-1_0-21.html#ProviderConfig

      if (!issuer.startsWith("https://")) {
        issuer = "https://" + issuer;
      }

      String wellKnownConfigUrl = issuer;

      if (!wellKnownConfigUrl.endsWith("/")) {
        wellKnownConfigUrl += "/";
      }

      String response = webClient
              .get(wellKnownConfigUrl + ".well-known/openid-configuration")
              .aggregate()
              .join()
              .contentUtf8();

      // TODO: We should cache this. No need to fetch it each time.
      Map<String, Object> configMap = mapper.readValue(response, new TypeReference<>() {
      });

      if (configMap == null || configMap.isEmpty()) {
        throw new OAuthInvalidRequestException(ErrorCode.ABORTED, "Could not get issuer configuration");
      }

      String configIssuer = (String) configMap.get("issuer");
      String configJwksUri = (String) configMap.get("jwks_uri");

      if (!configIssuer.equals(issuer)) {
        throw new OAuthInvalidRequestException(ErrorCode.ABORTED, "Issuer doesn't match configuration");
      }

      if (configJwksUri == null) {
        throw new OAuthInvalidRequestException(ErrorCode.ABORTED, "JWKS configuration missing");
      }

      // TODO: Or maybe just cache the provider for reuse.
      return new JwkProviderBuilder(new URL(configJwksUri)).cached(false).build();
    }
  }
}
