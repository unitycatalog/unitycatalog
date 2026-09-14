package io.unitycatalog.server.security;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.auth0.jwt.JWT;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.AuthorizationException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.utils.JwksOperations;
import io.unitycatalog.server.utils.ServerProperties;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves UC identities for both HTTP requests and trusted in-process integrations. */
public final class UnityCatalogIdentityService {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnityCatalogIdentityService.class);

  private final JwksOperations jwksOperations;
  private final UserRepository users;
  private final ServerProperties properties;

  public UnityCatalogIdentityService(
      SecurityContext securityContext, Repositories repositories, ServerProperties properties) {
    this.jwksOperations = new JwksOperations(securityContext);
    this.users = repositories.getUserRepository();
    this.properties = properties;
  }

  /** Authenticates a bearer token, or derives its identity in authorization-disabled mode. */
  public Identity authenticate(String token) {
    if (token == null || token.isBlank()) {
      throw new AuthorizationException(ErrorCode.UNAUTHENTICATED, "No authorization found.");
    }
    return properties.isAuthorizationEnabled() ? verify(token) : decodeLeniently(token);
  }

  /**
   * Resolves a user named by a trusted in-process caller. No server secret is needed because this
   * method is not exposed over HTTP.
   */
  public Identity onBehalfOf(String userId, String fallbackName) {
    UUID id;
    try {
      id = UUID.fromString(userId);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new AuthorizationException(
          ErrorCode.PERMISSION_DENIED, "Missing or invalid on-behalf-of user id.");
    }
    if (!properties.isAuthorizationEnabled()) {
      return new Identity(id, fallbackName, null);
    }
    User user = enabledUser(id.toString(), null);
    if (user == null) {
      throw new AuthorizationException(
          ErrorCode.PERMISSION_DENIED, "On-behalf-of user not allowed: " + id);
    }
    return new Identity(id, user.getEmail(), null);
  }

  private Identity verify(String token) {
    DecodedJWT decoded;
    try {
      decoded = JWT.decode(token);
      if (!INTERNAL.equals(decoded.getIssuer())) {
        throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "Invalid access token.");
      }
      decoded =
          jwksOperations
              .verifierForIssuerAndKey(
                  decoded.getIssuer(), decoded.getKeyId(), decoded.getAlgorithm())
              .verify(decoded);
    } catch (AuthorizationException e) {
      throw e;
    } catch (RuntimeException e) {
      LOGGER.debug("Bearer token validation failed", e);
      throw new AuthorizationException(ErrorCode.UNAUTHENTICATED, "Invalid access token.");
    }

    User user = enabledUser(null, decoded.getSubject());
    if (user == null) {
      throw new AuthorizationException(
          ErrorCode.PERMISSION_DENIED, "User not allowed: " + decoded.getSubject());
    }
    return new Identity(UUID.fromString(user.getId()), user.getEmail(), decoded);
  }

  private User enabledUser(String id, String email) {
    User user;
    try {
      user = id != null ? users.getUser(id) : users.getUserByEmail(email);
    } catch (RuntimeException e) {
      LOGGER.debug("User not found: {}", id != null ? id : email);
      return null;
    }
    return user != null && user.getState() == User.StateEnum.ENABLED ? user : null;
  }

  private Identity decodeLeniently(String token) {
    String subject = token;
    try {
      String claimed = JWT.decode(token).getSubject();
      if (claimed != null && !claimed.isBlank()) {
        subject = claimed;
      }
    } catch (JWTDecodeException e) {
      LOGGER.debug("Token is not JWT-shaped; using it as the subject", e);
    }
    return new Identity(
        UUID.nameUUIDFromBytes(subject.getBytes(StandardCharsets.UTF_8)), subject, null);
  }

  /** Authenticated UC principal and the decoded token, when verification was enabled. */
  public record Identity(UUID id, String name, DecodedJWT decodedJwt) {}
}
