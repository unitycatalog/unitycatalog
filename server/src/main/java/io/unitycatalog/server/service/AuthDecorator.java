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
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TokenRevocationRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.JwksOperations;
import java.util.UUID;
import org.hibernate.SessionFactory;
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
  private final UserRepository userRepository;
  private final TokenRevocationRepository tokenRevocationRepository;
  private final SessionFactory sessionFactory;

  public static final String UC_TOKEN_KEY = "UC_TOKEN";

  private static final String BEARER_PREFIX = "Bearer ";

  public static final AttributeKey<DecodedJWT> DECODED_JWT_ATTR =
      AttributeKey.valueOf(DecodedJWT.class, "DECODED_JWT_ATTR");

  private final JwksOperations jwksOperations;

  public AuthDecorator(SecurityContext securityContext, Repositories repositories) {
    this.jwksOperations = new JwksOperations(securityContext);
    this.userRepository = repositories.getUserRepository();
    this.tokenRevocationRepository = repositories.getTokenRevocationRepository();
    this.sessionFactory = repositories.getSessionFactory();
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

    DecodedJWT decodedJWT =
        JWT.decode(getAccessTokenFromCookieOrAuthHeader(authorizationHeader, authorizationCookie));

    String issuer = decodedJWT.getIssuer();
    String keyId = decodedJWT.getKeyId();
    String alg = decodedJWT.getAlgorithm();

    LOGGER.debug("Validating access-token for issuer: {} and keyId: {}", issuer, keyId);

    if (!issuer.equals(INTERNAL)) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "Invalid access token.");
    }

    // Internal tokens don't need audience validation
    JWTVerifier jwtVerifier = jwksOperations.verifierForIssuerAndKey(issuer, keyId, alg);
    DecodedJWT verifiedJWT = jwtVerifier.verify(decodedJWT);

    String subject = verifiedJWT.getSubject();
    // Tokens this server issues always carry a UUID jti; an absent or non-UUID jti fails here
    // (acceptable, since only issued tokens are revoked).
    UUID jti = UUID.fromString(verifiedJWT.getId());

    // Check token revocation and look up the user in a single transaction. This runs on every
    // authenticated request, so sharing one session keeps the auth path to a single connection
    // checkout. Expired tokens are already rejected by the verifier above, so only live tokens
    // reach the revocation check; a revoked token (e.g. from logout) is denied before the lookup.
    UserDAO userDAO =
        TransactionManager.executeWithTransaction(
            sessionFactory,
            session -> {
              if (tokenRevocationRepository.isRevoked(session, jti)) {
                throw new AuthorizationException(
                    ErrorCode.PERMISSION_DENIED, "Access token has been revoked.");
              }
              return userRepository.getUserByEmail(session, subject);
            },
            "Failed to authorize request",
            /* readOnly = */ true);

    if (userDAO == null) {
      throw new AuthorizationException(
          ErrorCode.PERMISSION_DENIED, "User does not exist: " + subject);
    }
    User user = userDAO.toUser();
    if (user.getState() != User.StateEnum.ENABLED) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "User not allowed: " + subject);
    }

    LOGGER.debug("Access allowed for subject: {}", subject);

    ctx.setAttr(DECODED_JWT_ATTR, verifiedJWT);

    return delegate.serve(ctx, req);
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
