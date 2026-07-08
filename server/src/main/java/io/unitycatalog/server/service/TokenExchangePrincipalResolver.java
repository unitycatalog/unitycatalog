package io.unitycatalog.server.service;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.unitycatalog.control.model.TokenType;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.utils.AudienceAllowlist;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves UC principals and validates subject token audiences during token exchange. */
public class TokenExchangePrincipalResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TokenExchangePrincipalResolver.class);

  private final ServerProperties serverProperties;
  private final UserRepository userRepository;

  public TokenExchangePrincipalResolver(
      ServerProperties serverProperties, UserRepository userRepository) {
    this.serverProperties = serverProperties;
    this.userRepository = userRepository;
  }

  /**
   * Validates audience rules and returns the UC principal email for the issued access token.
   *
   * <p>For {@code id_token} subjects, resolution order is email (or {@code sub}) then OAuth client
   * id from {@code azp} or non-URL {@code aud} mapped to {@code externalId}. For {@code
   * access_token} subjects without an {@code email} claim, {@code externalId} is tried before
   * {@code sub}.
   */
  public String resolvePrincipalEmail(TokenType subjectTokenType, DecodedJWT decodedJWT) {
    validateAudience(subjectTokenType, decodedJWT);

    if (subjectTokenType == TokenType.ACCESS_TOKEN && !hasEmailClaim(decodedJWT)) {
      Optional<String> clientPrincipal = tryResolvePrincipalEmailForClient(decodedJWT);
      if (clientPrincipal.isPresent()) {
        return clientPrincipal.get();
      }

      Optional<String> principalEmail = tryResolvePrincipalEmailFromTokenClaims(decodedJWT);
      if (principalEmail.isPresent()) {
        return principalEmail.get();
      }

      throw new OAuthInvalidRequestException(ErrorCode.INVALID_ARGUMENT, "User not allowed");
    }

    Optional<String> principalEmail = tryResolvePrincipalEmailFromTokenClaims(decodedJWT);
    if (principalEmail.isPresent()) {
      return principalEmail.get();
    }

    Optional<String> clientPrincipal = tryResolvePrincipalEmailForClient(decodedJWT);
    if (clientPrincipal.isPresent()) {
      return clientPrincipal.get();
    }

    throw new OAuthInvalidRequestException(ErrorCode.INVALID_ARGUMENT, "User not allowed");
  }

  private Optional<String> tryResolvePrincipalEmailForClient(DecodedJWT decodedJWT) {
    Optional<String> clientId = extractOAuthClientId(decodedJWT);
    if (clientId.isEmpty()) {
      return Optional.empty();
    }
    return tryResolvePrincipalEmailForClient(clientId.get());
  }

  static boolean hasEmailClaim(DecodedJWT decodedJWT) {
    Claim email = decodedJWT.getClaim(JwtClaim.EMAIL.key());
    return !email.isNull() && email.asString() != null && !email.asString().isBlank();
  }

  /**
   * Returns the OAuth client id from the subject token, preferring {@code azp} over {@code aud}.
   *
   * <p>URL-like audience values are skipped so realm-wide {@code aud} entries do not shadow the
   * per-client id.
   */
  static Optional<String> extractOAuthClientId(DecodedJWT decodedJWT) {
    Claim azp = decodedJWT.getClaim("azp");
    if (!azp.isNull()) {
      String authorizedParty = azp.asString();
      if (authorizedParty != null && !authorizedParty.isBlank()) {
        return Optional.of(authorizedParty);
      }
    }

    List<String> audiences = decodedJWT.getAudience();
    if (audiences == null) {
      return Optional.empty();
    }

    return audiences.stream()
        .filter(TokenExchangePrincipalResolver::isOAuthClientIdAudience)
        .findFirst();
  }

  private static boolean isOAuthClientIdAudience(String audience) {
    if (audience == null || audience.isBlank()) {
      return false;
    }
    return !audience.contains("://");
  }

  private void validateAudience(TokenType subjectTokenType, DecodedJWT decodedJWT) {
    List<String> audiences = serverProperties.getAudiences();
    Optional<String> tokenClientId = extractOAuthClientId(decodedJWT);

    if (audiences.isEmpty() && tokenClientId.isEmpty()) {
      LOGGER.error("No audiences configured");
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT,
          "No audiences configured. Set server.audiences in server.properties");
    }

    if (audiences.isEmpty()) {
      if (tokenClientId.isPresent() && hasEnabledUserForExternalId(tokenClientId.get())) {
        return;
      }
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT,
          "No audiences configured. Set server.audiences in server.properties");
    }

    if (serverProperties.isAudienceValidationDisabled()) {
      return;
    }

    if (AudienceAllowlist.isAllowed(decodedJWT.getAudience(), audiences)) {
      return;
    }

    if (subjectTokenType == TokenType.ID_TOKEN) {
      String configuredClientId = serverProperties.get(ServerProperties.Property.CLIENT_ID);
      if (configuredClientId != null
          && subjectTokenReferencesClient(decodedJWT, configuredClientId)) {
        return;
      }
    }

    if (tokenClientId.isPresent() && hasEnabledUserForExternalId(tokenClientId.get())) {
      return;
    }

    LOGGER.debug("Token rejected: audience not in allowlist");
    throw new OAuthInvalidRequestException(ErrorCode.UNAUTHENTICATED, "Invalid audience");
  }

  private boolean hasEnabledUserForExternalId(String externalId) {
    try {
      User user = userRepository.getUserByExternalId(externalId);
      return user != null && user.getState() == User.StateEnum.ENABLED;
    } catch (Exception e) {
      return false;
    }
  }

  private static boolean subjectTokenReferencesClient(DecodedJWT decodedJWT, String clientId) {
    Claim azp = decodedJWT.getClaim("azp");
    if (!azp.isNull() && clientId.equals(azp.asString())) {
      return true;
    }
    List<String> audiences = decodedJWT.getAudience();
    return audiences != null && audiences.contains(clientId);
  }

  private Optional<String> tryResolvePrincipalEmailForClient(String clientId) {
    LOGGER.debug("Resolving principal for OAuth client id {}", clientId);
    try {
      User user = userRepository.getUserByExternalId(clientId);
      if (user != null && user.getState() == User.StateEnum.ENABLED) {
        LOGGER.debug("Principal for client {} resolved to {}", clientId, user.getEmail());
        return Optional.of(user.getEmail());
      }
    } catch (Exception e) {
      // IGNORE
    }
    return Optional.empty();
  }

  private Optional<String> tryResolvePrincipalEmailFromTokenClaims(DecodedJWT decodedJWT) {
    String subject =
        decodedJWT
            .getClaims()
            .getOrDefault(JwtClaim.EMAIL.key(), decodedJWT.getClaim(JwtClaim.SUBJECT.key()))
            .asString();

    if (subject == null || subject.isBlank()) {
      return Optional.empty();
    }

    LOGGER.debug("Trying principal resolution from token claims: {}", subject);

    if ("admin".equals(subject)) {
      LOGGER.debug("admin always allowed");
      return Optional.of(subject);
    }

    try {
      User user = userRepository.getUserByEmail(subject);
      if (user != null && user.getState() == User.StateEnum.ENABLED) {
        LOGGER.debug("Principal {} resolved from token email", subject);
        return Optional.of(user.getEmail());
      }
    } catch (Exception e) {
      // IGNORE
    }

    return Optional.empty();
  }
}
