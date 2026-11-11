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
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.JwksOperations;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.EnumUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple JWT access-token authorization decorator.
 *
 * <p>This decorator accepts an Authorization header with a Bearer token, or a UC_TOKEN cookie when
 * no Bearer token is present. Cookie requests other than GET, HEAD, and OPTIONS require
 * X-Requested-With: XMLHttpRequest. The token is verified to be from the "internal" issuer and the
 * token signature is checked against the internal issuer key. The token must carry a known {@code
 * type}, and an {@code ACCESS} or {@code SCIM} token must carry an expiry; the bootstrap {@code
 * SERVICE} token has none. The token's user must be enabled. A {@code SCIM} token, an IdP's token
 * from {@code bin/uc-admin-token}, is accepted only on the SCIM API, and names its user by id; that
 * user must have an IdP name. If all these checks pass, the request is allowed to continue.
 *
 * <p>The decoded token is also added to the request attributes so it can be referenced by the
 * request if needed.
 */
public class AuthDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthDecorator.class);
  private final UserRepository userRepository;

  public static final String UC_TOKEN_KEY = "UC_TOKEN";

  private static final String BEARER_PREFIX = "Bearer ";

  /** The only paths that accept a SCIM token. */
  private static final String SCIM_PATH_PREFIX = "/api/1.0/unity-control/scim2/";

  private static final Set<HttpMethod> SAFE_METHODS =
      Set.of(HttpMethod.GET, HttpMethod.HEAD, HttpMethod.OPTIONS);

  public static final AttributeKey<DecodedJWT> DECODED_JWT_ATTR =
      AttributeKey.valueOf(DecodedJWT.class, "DECODED_JWT_ATTR");

  private final JwksOperations jwksOperations;

  public AuthDecorator(SecurityContext securityContext, Repositories repositories) {
    this.jwksOperations = new JwksOperations(securityContext);
    this.userRepository = repositories.getUserRepository();
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
    decodedJWT = jwtVerifier.verify(decodedJWT);
    boolean isScimToken = validateTypeAndExpiry(decodedJWT) == JwtTokenType.SCIM;
    if (isScimToken && !ctx.path().startsWith(SCIM_PATH_PREFIX)) {
      throw new AuthorizationException(
          ErrorCode.PERMISSION_DENIED,
          "A SCIM token is accepted only under " + SCIM_PATH_PREFIX + ".");
    }

    String subject = decodedJWT.getSubject();

    // A SCIM token names its principal by the user's id, other tokens by email.
    User user =
        findUser(
            subject,
            () ->
                isScimToken
                    ? userRepository.getUser(subject)
                    : userRepository.getUserByEmail(subject));
    if (user == null
        || user.getState() != User.StateEnum.ENABLED
        || (isScimToken && user.getIdpName() == null)) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "User not allowed: " + subject);
    }

    LOGGER.debug("Access allowed for subject: {}", subject);

    ctx.setAttr(DECODED_JWT_ATTR, decodedJWT);

    return delegate.serve(ctx, req);
  }

  /** Returns the user, or null when there is none; a failed lookup propagates. */
  private static User findUser(String subject, Supplier<User> lookup) {
    try {
      return lookup.get();
    } catch (BaseException failure) {
      // A missing user is a denial. Anything other than NOT_FOUND is a server failure, not a
      // denial; let it propagate.
      if (failure.getErrorCode() != ErrorCode.NOT_FOUND) {
        throw failure;
      }
      LOGGER.debug("User not found: {}", subject);
      return null;
    }
  }

  private static JwtTokenType validateTypeAndExpiry(DecodedJWT jwt) {
    JwtTokenType type =
        EnumUtils.getEnum(JwtTokenType.class, jwt.getClaim(JwtClaim.TOKEN_TYPE.key()).asString());
    if (type == null) {
      throw new AuthorizationException(
          ErrorCode.UNAUTHENTICATED,
          "Access token has a missing or unknown type. Get a new token from this server.");
    }
    if (type != JwtTokenType.SERVICE && jwt.getExpiresAt() == null) {
      throw new AuthorizationException(
          ErrorCode.UNAUTHENTICATED,
          "Access token has no expiry. Get a new token from this server.");
    }
    return type;
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
