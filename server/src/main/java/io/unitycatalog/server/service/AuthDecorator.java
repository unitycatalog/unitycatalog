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
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.JwksOperations;
import io.unitycatalog.server.utils.ServerProperties;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;
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
 *
 * <p>A second, unrelated way in is on-behalf-of access (see {@link #ON_BEHALF_OF_USER_ID_ATTR}):
 * OpenSharing's own server identity, not any user's token, naming which user's privileges a request
 * should be evaluated against. Exists for exactly one situation — a recipient's read has to reach
 * the catalog as the share's owner, and no owner token is available to present for it, because the
 * owner is not the one asking and never will be for this request.
 */
public class AuthDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthDecorator.class);
  private final UserRepository userRepository;
  private final ServerProperties serverProperties;

  public static final String UC_TOKEN_KEY = "UC_TOKEN";

  private static final String BEARER_PREFIX = "Bearer ";

  /** OpenSharing's own credential, proving the request comes from it and not from anybody else. */
  public static final String OPENSHARING_SERVER_SECRET_HEADER = "X-OpenSharing-Server-Secret";

  /** The UC user id this request should be evaluated as, when presented with the header above. */
  public static final String OPENSHARING_ON_BEHALF_OF_HEADER = "X-OpenSharing-On-Behalf-Of";

  public static final AttributeKey<DecodedJWT> DECODED_JWT_ATTR =
      AttributeKey.valueOf(DecodedJWT.class, "DECODED_JWT_ATTR");

  /** Set instead of {@link #DECODED_JWT_ATTR} for a validated on-behalf-of request. */
  public static final AttributeKey<UUID> ON_BEHALF_OF_USER_ID_ATTR =
      AttributeKey.valueOf(UUID.class, "ON_BEHALF_OF_USER_ID_ATTR");

  private final JwksOperations jwksOperations;

  public AuthDecorator(
      SecurityContext securityContext,
      Repositories repositories,
      ServerProperties serverProperties) {
    this.jwksOperations = new JwksOperations(securityContext);
    this.userRepository = repositories.getUserRepository();
    this.serverProperties = serverProperties;
  }

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
      throws Exception {
    LOGGER.debug("AuthDecorator checking {}", req.path());

    String serverSecret = req.headers().get(OPENSHARING_SERVER_SECRET_HEADER);
    if (serverSecret != null) {
      return serveOnBehalfOf(delegate, ctx, req, serverSecret);
    }

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

    return delegate.serve(ctx, req);
  }

  /**
   * Validates the server secret against configuration and the named user against the user
   * repository, then continues the request as that user rather than as whoever presented the secret
   * — the secret proves this is OpenSharing asking, not who it is asking for. Rejected outright on
   * any mismatch, rather than falling through to a normal bearer-token check: a caller that sent
   * this header meant to use on-behalf-of access, and a normal-auth retry of the same request would
   * only obscure which of the two checks it failed.
   */
  private HttpResponse serveOnBehalfOf(
      HttpService delegate, ServiceRequestContext ctx, HttpRequest req, String presentedSecret)
      throws Exception {
    String configuredSecret = serverProperties.getOpenSharingServerSecret();
    if (configuredSecret == null
        || configuredSecret.isBlank()
        || !constantTimeEquals(presentedSecret, configuredSecret)) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "Invalid server identity.");
    }
    String onBehalfOf = req.headers().get(OPENSHARING_ON_BEHALF_OF_HEADER);
    UUID userId;
    try {
      userId = UUID.fromString(onBehalfOf);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new AuthorizationException(
          ErrorCode.PERMISSION_DENIED, "Missing or invalid " + OPENSHARING_ON_BEHALF_OF_HEADER);
    }
    User user;
    try {
      user = userRepository.getUser(userId.toString());
    } catch (Exception e) {
      user = null;
    }
    if (user == null || user.getState() != User.StateEnum.ENABLED) {
      throw new AuthorizationException(
          ErrorCode.PERMISSION_DENIED, "On-behalf-of user not allowed: " + userId);
    }
    LOGGER.debug("On-behalf-of access allowed for user: {}", userId);
    ctx.setAttr(ON_BEHALF_OF_USER_ID_ATTR, userId);
    return delegate.serve(ctx, req);
  }

  private static boolean constantTimeEquals(String a, String b) {
    return MessageDigest.isEqual(
        a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
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
