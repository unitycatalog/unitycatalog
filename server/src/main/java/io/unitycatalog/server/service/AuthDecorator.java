package io.unitycatalog.server.service;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.interfaces.DecodedJWT;
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
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.utils.JwksOperations;
import java.util.Map;
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
  private static final UserRepository USER_REPOSITORY = UserRepository.getInstance();

  public static final AttributeKey<DecodedJWT> DECODED_JWT_ATTR =
      AttributeKey.valueOf(DecodedJWT.class, "DECODED_JWT_ATTR");

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
      throws Exception {
    LOGGER.debug("AuthDecorator checking {}", req.path());
    String authorization =
        req.headers().stream()
            .filter(h -> h.getKey().equals(HttpHeaderNames.AUTHORIZATION))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);

    if (authorization == null) {
      throw new AuthorizationException(ErrorCode.UNAUTHENTICATED, "No authorization found.");
    }

    String[] parts = authorization.split(" ");

    if (parts.length != 2 || !parts[0].equals("Bearer")) {
      throw new AuthorizationException(ErrorCode.UNAUTHENTICATED, "No Bearer found.");
    } else {
      String token = parts[1];
      DecodedJWT decodedJWT = JWT.decode(token);

      JwksOperations jwksOperations = new JwksOperations();

      String issuer = decodedJWT.getClaim(JwtClaim.ISSUER.key()).asString();
      String keyId = decodedJWT.getHeaderClaim(JwtClaim.KEY_ID.key()).asString();

      LOGGER.debug("Validating access-token for issuer: {}", issuer);

      if (!issuer.equals(INTERNAL)) {
        throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "Invalid access token.");
      }

      JWTVerifier jwtVerifier = jwksOperations.verifierForIssuerAndKey(issuer, keyId);
      decodedJWT = jwtVerifier.verify(decodedJWT);

      User user;
      try {
        user =
            USER_REPOSITORY.getUserByEmail(decodedJWT.getClaim(JwtClaim.SUBJECT.key()).asString());
      } catch (Exception e) {
        user = null;
      }
      if (user == null || user.getState() != User.StateEnum.ENABLED) {
        throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "User not allowed.");
      }

      LOGGER.debug("Access allowed for subject: {}", decodedJWT.getClaim(JwtClaim.SUBJECT.key()));

      ctx.setAttr(DECODED_JWT_ATTR, decodedJWT);
    }

    return delegate.serve(ctx, req);
  }
}
