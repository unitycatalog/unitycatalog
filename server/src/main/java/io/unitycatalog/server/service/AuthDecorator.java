package io.unitycatalog.server.service;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.common.Cookie;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.netty.util.AttributeKey;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.AuthorizationException;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TokenRevocationRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.JwksOperations;
import java.util.Set;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple JWT access-token authorization decorator.
 *
 * <p>This decorator accepts an Authorization header with a Bearer token, or a UC_TOKEN cookie when
 * no Bearer token is present. Cookie requests other than GET, HEAD, and OPTIONS require
 * X-Requested-With: XMLHttpRequest. The token is verified to be from the "internal" issuer and the
 * token signature is checked against the internal issuer key. If all these checks pass, the request
 * is allowed to continue.
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
  private static final Set<HttpMethod> SAFE_METHODS =
      Set.of(HttpMethod.GET, HttpMethod.HEAD, HttpMethod.OPTIONS);

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
        JWT.decode(
            getAccessTokenFromCookieOrAuthHeader(
                authorizationHeader, authorizationCookie, req.headers()));

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
    UUID jti = parseJti(verifiedJWT);

    UserDAO userDAO = findUserUnlessRevoked(jti, subject);
    // The token may have expired, and its revocation row been purged, during the lookup above, so
    // check the expiry again. A purged row has expired, so this rejects it.
    jwtVerifier.verify(verifiedJWT);
    if (userDAO == null || userDAO.toUser().getState() != User.StateEnum.ENABLED) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "User not allowed: " + subject);
    }

    LOGGER.debug("Access allowed for subject: {}", subject);

    ctx.setAttr(DECODED_JWT_ATTR, verifiedJWT);

    return delegate.serve(ctx, req);
  }

  /**
   * Checks token revocation and looks up the user in one transaction. This runs on every
   * authenticated request, so sharing one session keeps the auth path to a single connection
   * checkout.
   *
   * @param jti the token's JWT ID
   * @param subject the token's subject, the user's email
   * @return the user, or {@code null} if there is none
   * @throws AuthorizationException with {@code UNAUTHENTICATED} if the token has been revoked
   * @throws BaseException if the database fails, without the database's message, which the error
   *     handler logs with the cause
   */
  private UserDAO findUserUnlessRevoked(UUID jti, String subject) {
    try {
      return TransactionManager.executeWithTransaction(
          sessionFactory,
          session -> {
            if (tokenRevocationRepository.isRevoked(session, jti)) {
              throw new AuthorizationException(
                  ErrorCode.UNAUTHENTICATED, "Access token has been revoked.");
            }
            return userRepository.getUserByEmail(session, subject);
          },
          "Failed to authorize request",
          /* readOnly= */ true);
    } catch (AuthorizationException e) {
      throw e;
    } catch (BaseException e) {
      throw new BaseException(e.getErrorCode(), "Failed to authorize request", e);
    }
  }

  /**
   * Returns the token's jti as a UUID. Tokens this server issues always carry a UUID jti, so a
   * missing or malformed one means the token is not one we issued.
   *
   * @param verifiedJWT a token whose signature has already been verified
   * @return the token's jti
   * @throws AuthorizationException with {@code UNAUTHENTICATED} if the jti is missing or not a UUID
   */
  static UUID parseJti(DecodedJWT verifiedJWT) {
    String jti = verifiedJWT.getId();
    if (jti != null) {
      try {
        return UUID.fromString(jti);
      } catch (IllegalArgumentException e) {
        LOGGER.debug("Access token has a non-UUID jti: {}", jti);
      }
    }
    throw new AuthorizationException(ErrorCode.UNAUTHENTICATED, "Invalid access token.");
  }

  private String getAccessTokenFromCookieOrAuthHeader(
      String authorizationHeader, String authorizationCookie, RequestHeaders headers) {
    if (authorizationHeader != null && authorizationHeader.startsWith(BEARER_PREFIX)) {
      return authorizationHeader.substring(BEARER_PREFIX.length());
    }
    if (authorizationCookie != null) {
      validateCsrfHeader(headers);
      return authorizationCookie;
    }
    throw new AuthorizationException(ErrorCode.UNAUTHENTICATED, "No authorization found.");
  }

  /** Requires a header that browsers cannot send across origins without CORS preflight approval. */
  static void validateCsrfHeader(RequestHeaders headers) {
    if (!SAFE_METHODS.contains(headers.method())
        && !"XMLHttpRequest".equals(headers.get(HttpHeaderNames.X_REQUESTED_WITH))) {
      throw new AuthorizationException(
          ErrorCode.PERMISSION_DENIED, "Cookie requests require X-Requested-With: XMLHttpRequest");
    }
  }
}
