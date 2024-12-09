package io.unitycatalog.server.utils;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

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
import java.net.URL;
import java.nio.file.Path;
import java.security.interfaces.RSAPublicKey;
import java.util.Map;

import io.unitycatalog.server.service.AuthService;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwksOperations {

  private final WebClient webClient = WebClient.builder().build();
  private static final ObjectMapper mapper = new ObjectMapper();
  private final SecurityContext securityContext;

  private static final Logger LOGGER = LoggerFactory.getLogger(JwksOperations.class);

  public JwksOperations(SecurityContext securityContext) {
    this.securityContext = securityContext;
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

      String response = webClient
              .get(path)
              .aggregate()
              .join()
              .contentUtf8();

      // TODO: We should cache this. No need to fetch it each time.
      Map<String, Object> configMap = mapper.readValue(response, new TypeReference<>() {});

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
