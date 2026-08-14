package io.unitycatalog.server.service;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.unitycatalog.control.model.TokenType;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.JwtClaim;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves UC principals from an already validated subject token during token exchange. */
public class TokenExchangeSubjectTokenHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TokenExchangeSubjectTokenHandler.class);

  private final UserRepository userRepository;

  public TokenExchangeSubjectTokenHandler(UserRepository userRepository) {
    this.userRepository = userRepository;
  }

  /**
   * Returns the UC principal email for the issued access token.
   *
   * <p>The subject token must already be issuer-, signature-, and audience-validated. For {@code
   * id_token} subjects, resolution order is email (or {@code sub}) then OAuth client id from {@code
   * azp} or {@code client_id} mapped to {@code externalId}. For {@code access_token} subjects
   * without an {@code email} claim, {@code externalId} is tried before {@code sub}.
   */
  public String resolvePrincipalEmail(TokenType subjectTokenType, DecodedJWT decodedJWT) {
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
