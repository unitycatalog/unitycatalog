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
import io.unitycatalog.server.persist.model.UserLink;
import io.unitycatalog.server.security.IdentityProvider;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.security.SubjectClaim;
import io.unitycatalog.server.utils.JwksOperations;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
 * user must have an IdP name, and only a SCIM token is accepted for a user with one. If all these
 * checks pass, the request is allowed to continue.
 *
 * <p>In {@code external} authentication mode the decorator also accepts an access token from a
 * configured identity provider, by its exact issuer: the signature (the issuer's JWKS), expiry and
 * audience are checked, and the token's subject claim finds the user linked to it. The first call
 * links the enabled row with the token's {@code email}; later calls match the link only. The
 * server's own ACCESS tokens are refused; its SERVICE and SCIM tokens still work.
 *
 * <p>The decoded token is also added to the request attributes so it can be referenced by the
 * request if needed.
 */
public class AuthDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthDecorator.class);
  private final UserRepository userRepository;

  public static final String UC_TOKEN_KEY = "UC_TOKEN";

  private static final String BEARER_PREFIX = "Bearer ";

  private static final String EMAIL_CLAIM = "email";

  /** The only paths that accept a SCIM token. */
  private static final String SCIM_PATH_PREFIX = "/api/1.0/unity-control/scim2/";

  private static final Set<HttpMethod> SAFE_METHODS =
      Set.of(HttpMethod.GET, HttpMethod.HEAD, HttpMethod.OPTIONS);

  public static final AttributeKey<DecodedJWT> DECODED_JWT_ATTR =
      AttributeKey.valueOf(DecodedJWT.class, "DECODED_JWT_ATTR");

  /** The email of the user an identity provider's token is linked to; its {@code sub} isn't one. */
  public static final AttributeKey<String> PRINCIPAL_EMAIL_ATTR =
      AttributeKey.valueOf(String.class, "PRINCIPAL_EMAIL_ATTR");

  private final JwksOperations jwksOperations;
  private final boolean isExternalAuthentication;
  // Empty in internal mode.
  private final Map<String, IdentityProvider> identityProvidersByIssuer;

  public AuthDecorator(
      SecurityContext securityContext,
      Repositories repositories,
      ServerProperties serverProperties) {
    this.jwksOperations =
        new JwksOperations(securityContext, serverProperties.getAllowedJwksUris());
    this.userRepository = repositories.getUserRepository();
    this.isExternalAuthentication = serverProperties.isExternalAuthentication();
    List<IdentityProvider> identityProviders =
        isExternalAuthentication ? serverProperties.getIdentityProviders() : List.of();
    // A HashMap, so a token without an issuer finds none.
    this.identityProvidersByIssuer =
        identityProviders.stream()
            .collect(Collectors.toMap(IdentityProvider::issuer, Function.identity()));
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

    IdentityProvider identityProvider = identityProvidersByIssuer.get(issuer);
    if (identityProvider != null) {
      decodedJWT = jwksOperations.verifierForIssuerAndKey(issuer, keyId, alg).verify(decodedJWT);
      User user = linkedUser(identityProvider, decodedJWT);
      ctx.setAttr(DECODED_JWT_ATTR, decodedJWT);
      ctx.setAttr(PRINCIPAL_EMAIL_ATTR, user.getEmail());
      return delegate.serve(ctx, req);
    }

    if (!INTERNAL.equals(issuer)) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "Invalid access token.");
    }

    // Internal tokens don't need audience validation
    JWTVerifier jwtVerifier = jwksOperations.verifierForIssuerAndKey(issuer, keyId, alg);
    decodedJWT = jwtVerifier.verify(decodedJWT);
    JwtTokenType type = validateTypeAndExpiry(decodedJWT);
    // In external mode people and machines send their IdP's tokens; only the bootstrap admin's
    // SERVICE token and IdP principals' SCIM tokens come from this server.
    if (isExternalAuthentication && type == JwtTokenType.ACCESS) {
      throw new AuthorizationException(
          ErrorCode.UNAUTHENTICATED,
          "Access tokens from this server are not accepted in external authentication mode; send"
              + " the identity provider's access token.");
    }
    boolean isScimToken = type == JwtTokenType.SCIM;
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
        // Only a SCIM token acts as an IdP principal, and a SCIM token only as one.
        || isScimToken != (user.getIdpName() != null)) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "User not allowed: " + subject);
    }

    LOGGER.debug("Access allowed for subject: {}", subject);

    ctx.setAttr(DECODED_JWT_ATTR, decodedJWT);

    return delegate.serve(ctx, req);
  }

  /**
   * Returns the enabled user linked to a verified token of {@code identityProvider}. The first call
   * links the row with the token's {@code email}; later calls use the link only, whatever the
   * token's email.
   */
  private User linkedUser(IdentityProvider identityProvider, DecodedJWT jwt) {
    if (jwt.getExpiresAt() == null) {
      throw new AuthorizationException(ErrorCode.UNAUTHENTICATED, "Access token has no expiry.");
    }
    if (!jwt.getAudience().contains(identityProvider.audience())) {
      throw new AuthorizationException(ErrorCode.UNAUTHENTICATED, "Invalid audience");
    }
    SubjectClaim subjectClaim = identityProvider.subjectClaim();
    UserLink link = subjectClaim.linkOf(identityProvider.issuer(), jwt);
    if (link == null) {
      throw new AuthorizationException(
          ErrorCode.UNAUTHENTICATED,
          "Access token has no " + subjectClaim.claimNames() + " claim.");
    }
    User user = userRepository.findLinkedUser(link);
    if (user == null) {
      String email = jwt.getClaim(EMAIL_CLAIM).asString();
      if (email == null) {
        throw new AuthorizationException(
            ErrorCode.UNAUTHENTICATED,
            "Access token has no email claim to link a user; add one in the identity provider.");
      }
      // A concurrent first call may have linked the row first; then the link finds it.
      user =
          Optional.ofNullable(userRepository.linkUser(email, link))
              .orElseGet(() -> userRepository.findLinkedUser(link));
      // TODO(PR 11): with no row for this email, create one owned by this identity provider.
      if (user == null) {
        throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "User not allowed: " + email);
      }
      LOGGER.debug("Linked user {} to identity provider {}", email, identityProvider.name());
    }
    if (user.getState() != User.StateEnum.ENABLED) {
      throw new AuthorizationException(
          ErrorCode.PERMISSION_DENIED, "User not allowed: " + user.getEmail());
    }
    return user;
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
