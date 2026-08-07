package io.unitycatalog.server.service;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.unitycatalog.control.model.TokenType;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Validates subject token audiences and resolves UC principals during token exchange. */
public class TokenExchangeSubjectTokenHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TokenExchangeSubjectTokenHandler.class);

  private final ServerProperties serverProperties;
  private final UserRepository userRepository;

  public TokenExchangeSubjectTokenHandler(
      ServerProperties serverProperties, UserRepository userRepository) {
    this.serverProperties = serverProperties;
    this.userRepository = userRepository;
  }

  /**
   * Validates audience rules and returns the UC principal email for the issued access token.
   *
   * <p>For {@code id_token} subjects, resolution order is email (or {@code sub}) then OAuth client
   * id from {@code azp} or {@code client_id} mapped to {@code externalId}. For {@code access_token}
   * subjects without an {@code email} claim, {@code externalId} is tried before {@code sub}.
   */
  public String resolvePrincipalEmail(TokenType subjectTokenType, DecodedJWT decodedJWT) {
    validateAudience(subjectTokenType, decodedJWT);

    PrincipalResolutionOrder order =
        subjectTokenType == TokenType.ACCESS_TOKEN && !hasEmailClaim(decodedJWT)
            ? PrincipalResolutionOrder.CLIENT_THEN_CLAIMS
            : PrincipalResolutionOrder.CLAIMS_THEN_CLIENT;

    return resolveInOrder(decodedJWT, order);
  }

  private String resolveInOrder(DecodedJWT decodedJWT, PrincipalResolutionOrder order) {
    for (ResolutionStep step : order.steps()) {
      Optional<String> email =
          step == ResolutionStep.CLAIMS
              ? tryResolvePrincipalEmailFromTokenClaims(decodedJWT)
              : tryResolvePrincipalEmailForClient(decodedJWT);
      if (email.isPresent()) {
        return email.get();
      }
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

  /** Returns the OAuth client id from {@code azp}, else {@code client_id}. */
  static Optional<String> extractOAuthClientId(DecodedJWT decodedJWT) {
    Claim azp = decodedJWT.getClaim("azp");
    if (!azp.isNull()) {
      String authorizedParty = azp.asString();
      if (authorizedParty != null && !authorizedParty.isBlank()) {
        return Optional.of(authorizedParty);
      }
    }

    Claim clientId = decodedJWT.getClaim("client_id");
    if (!clientId.isNull()) {
      String clientIdValue = clientId.asString();
      if (clientIdValue != null && !clientIdValue.isBlank()) {
        return Optional.of(clientIdValue);
      }
    }

    return Optional.empty();
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
      if (tokenClientId.isPresent()
          && findEnabledUserByExternalId(tokenClientId.get()).isPresent()) {
        return;
      }
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT,
          "No audiences configured. Set server.audiences in server.properties");
    }

    if (serverProperties.isAudienceValidationDisabled()) {
      return;
    }

    if (serverProperties.getAudienceAllowlist().isAnyAllowed(decodedJWT.getAudience())) {
      return;
    }

    if (subjectTokenType == TokenType.ID_TOKEN) {
      String configuredClientId = serverProperties.get(ServerProperties.Property.CLIENT_ID);
      if (configuredClientId != null
          && subjectTokenReferencesClient(decodedJWT, configuredClientId)) {
        return;
      }
    }

    if (tokenClientId.isPresent() && findEnabledUserByExternalId(tokenClientId.get()).isPresent()) {
      return;
    }

    LOGGER.debug("Token rejected: audience not in allowlist");
    throw new OAuthInvalidRequestException(ErrorCode.UNAUTHENTICATED, "Invalid audience");
  }

  private Optional<User> findEnabledUserByExternalId(String externalId) {
    try {
      User user = userRepository.getUserByExternalId(externalId);
      if (user != null && user.getState() == User.StateEnum.ENABLED) {
        return Optional.of(user);
      }
    } catch (Exception e) {
      // IGNORE
    }
    return Optional.empty();
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
    Optional<String> email = findEnabledUserByExternalId(clientId).map(User::getEmail);
    email.ifPresent(
        resolvedEmail ->
            LOGGER.debug("Principal for client {} resolved to {}", clientId, resolvedEmail));
    return email;
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

  private enum ResolutionStep {
    CLAIMS,
    CLIENT
  }

  private enum PrincipalResolutionOrder {
    CLAIMS_THEN_CLIENT(ResolutionStep.CLAIMS, ResolutionStep.CLIENT),
    CLIENT_THEN_CLAIMS(ResolutionStep.CLIENT, ResolutionStep.CLAIMS);

    private final ResolutionStep[] steps;

    PrincipalResolutionOrder(ResolutionStep... steps) {
      this.steps = steps;
    }

    ResolutionStep[] steps() {
      return steps;
    }
  }
}
