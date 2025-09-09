package io.unitycatalog.server.service;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.common.Cookie;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.netty.util.AttributeKey;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.AuthorizationException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.DeveloperTokenRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.dao.DeveloperTokenDAO;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.JwksOperations;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple JWT access-token authorization decorator.
 *
 * <p>This decorator implements simple authorization. It requires an Authorization header in the
 * request with a Bearer token. The token is verified to be from the "internal" issuer and the token
 * signature is checked against the internal issuer key. If all these checks pass, the request is
 * allowed to continue.
 *
 * <p>The decoded token is also added to the request attributes so it can be referenced by the
 * request if needed.
 */
public class AuthDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthDecorator.class);

  public static final String UC_TOKEN_KEY = "UC_TOKEN";

  private static final String BEARER_PREFIX = "Bearer ";

  public static final AttributeKey<DecodedJWT> DECODED_JWT_ATTR =
      AttributeKey.valueOf(DecodedJWT.class, "DECODED_JWT_ATTR");

  public static final AttributeKey<User> PRINCIPAL_ATTR =
      AttributeKey.valueOf(User.class, "PRINCIPAL_ATTR");

  private final JwksOperations jwksOperations;
  private final UserRepository userRepository;
  private final DeveloperTokenRepository developerTokenRepository;

  public AuthDecorator(SecurityContext securityContext, Repositories repositories) {
    this.jwksOperations = new JwksOperations(securityContext);
    this.userRepository = repositories.getUserRepository();
    this.developerTokenRepository = repositories.getDeveloperTokenRepository();
  }

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
      throws Exception {
    LOGGER.debug("AuthDecorator checking {}", req.path());

    String authorizationHeader = req.headers().get(HttpHeaderNames.AUTHORIZATION);
    String authorizationCookie =
        req.headers().cookies().stream()
            .filter(c -> c.name().equals(UC_TOKEN_KEY))
            .map(Cookie::value)
            .findFirst()
            .orElse(null);

    String token = getAccessTokenFromCookieOrAuthHeader(authorizationHeader, authorizationCookie);

    // Check if this is a DAPI token (starts with "dapi_")
    if (token.startsWith("dapi_")) {
      return handleDapiTokenAuth(delegate, ctx, req, token);
    } else {
      return handleJwtTokenAuth(delegate, ctx, req, token);
    }
  }

  private HttpResponse handleDapiTokenAuth(
      HttpService delegate, ServiceRequestContext ctx, HttpRequest req, String dapiToken)
      throws Exception {
    LOGGER.debug("Validating DAPI token");

    // Hash the token to compare with stored hash
    String tokenHash = hashToken(dapiToken);

    // Look up the token in the database
    Optional<DeveloperTokenDAO> tokenOpt = developerTokenRepository.getTokenByHash(tokenHash);

    if (tokenOpt.isEmpty()) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "Invalid DAPI token.");
    }

    DeveloperTokenDAO tokenDAO = tokenOpt.get();

    // Check if token is active and not expired
    if (tokenDAO.getStatus() != DeveloperTokenDAO.TokenStatus.ACTIVE) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "DAPI token is revoked.");
    }

    if (tokenDAO.getExpiryTime().before(new java.util.Date())) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "DAPI token is expired.");
    }

    // Get the user associated with this token
    User user;
    try {
      user = userRepository.getUserByEmail(tokenDAO.getUserId());
    } catch (Exception e) {
      LOGGER.debug("User not found: {}", tokenDAO.getUserId());
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "DAPI token user not found.");
    }

    // Set user in context for authorization
    ctx.setAttr(PRINCIPAL_ATTR, user);

    return delegate.serve(ctx, req);
  }

  private HttpResponse handleJwtTokenAuth(
      HttpService delegate, ServiceRequestContext ctx, HttpRequest req, String token)
      throws Exception {
    DecodedJWT decodedJWT = JWT.decode(token);

    String issuer = decodedJWT.getIssuer();
    String keyId = decodedJWT.getKeyId();

    LOGGER.debug("Validating access-token for issuer: {} and keyId: {}", issuer, keyId);

    if (!issuer.equals(INTERNAL)) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "Invalid access token.");
    }

    JWTVerifier jwtVerifier = jwksOperations.verifierForIssuerAndKey(issuer, keyId);
    decodedJWT = jwtVerifier.verify(decodedJWT);

    String subject = decodedJWT.getSubject();

    User user;
    try {
      user = userRepository.getUserByEmail(subject);
    } catch (Exception e) {
      LOGGER.debug("User not found: {}", subject);
      user = null;
    }
    if (user == null || user.getState() != User.StateEnum.ENABLED) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "User not allowed: " + subject);
    }

    LOGGER.debug("Access allowed for subject: {}", subject);

    ctx.setAttr(DECODED_JWT_ATTR, decodedJWT);
    ctx.setAttr(PRINCIPAL_ATTR, user);

    return delegate.serve(ctx, req);
  }

  private String hashToken(String tokenValue) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hashBytes = digest.digest(tokenValue.getBytes(StandardCharsets.UTF_8));
      return Base64.getEncoder().encodeToString(hashBytes);
    } catch (Exception e) {
      throw new RuntimeException("Failed to hash token", e);
    }
  }

  private String getAccessTokenFromCookieOrAuthHeader(
      String authorizationHeader, String authorizationCookie) {
    if (authorizationHeader != null && authorizationHeader.startsWith(BEARER_PREFIX)) {
      return authorizationHeader.substring(BEARER_PREFIX.length());
    }
    if (authorizationCookie != null) {
      return authorizationCookie;
    }
    throw new AuthorizationException(ErrorCode.UNAUTHENTICATED, "No authorization found.");
  }
}
