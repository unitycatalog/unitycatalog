package io.unitycatalog.server.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.common.*;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.control.model.AccessTokenType;
import io.unitycatalog.control.model.GrantType;
import io.unitycatalog.control.model.OAuthTokenExchangeResponse;
import io.unitycatalog.control.model.TokenType;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.JwksOperations;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Duration;
import java.util.Optional;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExceptionHandler(GlobalExceptionHandler.class)
public class AuthService {
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthService.class);
  private static final UserRepository USER_REPOSITORY = UserRepository.getInstance();

  private final SecurityContext securityContext;
  private final JwksOperations jwksOperations;

  private static final String COOKIE = "cookie";
  private static final String EMPTY_RESPONSE = "{}";

  public AuthService(SecurityContext securityContext) {
    this.securityContext = securityContext;
    this.jwksOperations = new JwksOperations(securityContext);
  }

  /**
   * OAuth token exchange.
   *
   * <p>Performs an OAuth token exchange for an access-token. Specifically this endpoint accepts a
   * "token-exchange" grant type (urn:ietf:params:oauth:grant-type:token-exchange) and along with
   * either an identity-token (urn:ietf:params:oauth:token-type:id_token) or a access-token
   * (urn:ietf:params:oauth:token-type:access_token), validates the token signature using OIDC
   * discovery and JWKs, and then creates a new access-token.
   *
   * <ul>
   *   <li>grant_type: urn:ietf:params:oauth:grant-type:token-exchange
   *   <li>requested_token_type: urn:ietf:params:oauth:token-type:access_token or
   *       urn:ietf:params:oauth:token-type:id_token
   *   <li>subject_token_type: urn:ietf:params:oauth:token-type:access_token
   *   <li>subject_token: The incoming token (typically from an identity provider)
   *   <li>actor_token_type: Not supported
   *   <li>actor_token: Not supported
   *   <li>scope: Not supported
   * </ul>
   *
   * <p>Currently the issuer for the incoming token to validate is not constrained to a specific
   * identity provider, rather as long as the token is signed by the matching issuer the validation
   * succeeds.
   *
   * <p>Eventually this should be constrained to a specific identity provider and even require that
   * the incoming identity (email, subject) matches a specific user in the system, once a user
   * management system is in place.
   *
   * <p>NOTE: Armelia does not convert some `Enum` values properly; hence, we should treat them as
   * `String` here.
   *
   * <p>SEE:
   * https://armeria.dev/docs/server-annotated-service/#injecting-a-parameter-as-an-enum-type
   *
   * @param request The token exchange parameters
   * @return The token exchange response
   */
  @Post("/tokens")
  public HttpResponse grantToken(
      @Param("ext") Optional<String> ext, OAuthTokenExchangeRequest request) {
    LOGGER.debug("Got token: {}", request);

    if (request.getGrantType() == null
        || !GrantType.TOKEN_EXCHANGE.getValue().equals(request.getGrantType())) {
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT, "Unsupported grant type: " + request.getGrantType());
    }

    if (request.getRequestedTokenType() == null
        || !TokenType.ACCESS_TOKEN.getValue().equals(request.getRequestedTokenType())) {
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT,
          "Unsupported requested token type: " + request.getRequestedTokenType());
    }

    if (request.getSubjectTokenType() == null) {
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT, "Subject token type is required but was not specified");
    }

    if (request.getActorTokenType() != null) {
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT, "Actor tokens not currently supported");
    }

    boolean authorizationEnabled = ServerProperties.getInstance().isAuthorizationEnabled();
    if (!authorizationEnabled) {
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT, "Authorization is disabled");
    }

    DecodedJWT decodedJWT = JWT.decode(request.getSubjectToken());
    String issuer = decodedJWT.getIssuer();
    String keyId = decodedJWT.getKeyId();

    LOGGER.debug("Validating token for issuer: {} and keyId: {}", issuer, keyId);

    JWTVerifier jwtVerifier = jwksOperations.verifierForIssuerAndKey(issuer, keyId);
    decodedJWT = jwtVerifier.verify(decodedJWT);
    verifyPrincipal(decodedJWT);

    LOGGER.debug("Validated. Creating access token.");

    String accessToken = securityContext.createAccessToken(decodedJWT);

    OAuthTokenExchangeResponse response =
        new OAuthTokenExchangeResponse()
            .accessToken(accessToken)
            .issuedTokenType(TokenType.ACCESS_TOKEN)
            .tokenType(AccessTokenType.BEARER);

    // Set token as cookie if ext param is set to cookie
    ResponseHeadersBuilder responseHeaders = ResponseHeaders.builder(HttpStatus.OK);
    ext.ifPresent(
        e -> {
          if (e.equals(COOKIE)) {
            // Set cookie timeout to 5 days by default if not present in server.properties
            String cookieTimeout =
                ServerProperties.getInstance().getProperty("server.cookie-timeout", "P5D");
            Cookie cookie =
                createCookie(AuthDecorator.UC_TOKEN_KEY, accessToken, "/", cookieTimeout);
            responseHeaders.add(HttpHeaderNames.SET_COOKIE, cookie.toSetCookieHeader());
          }
        });

    return HttpResponse.ofJson(responseHeaders.build(), response);
  }

  @Post("/logout")
  public HttpResponse logout(HttpRequest request) {
    return request.headers().cookies().stream()
        .filter(c -> c.name().equals(AuthDecorator.UC_TOKEN_KEY))
        .findFirst()
        .map(
            authorizationCookie -> {
              Cookie expiredCookie = createCookie(AuthDecorator.UC_TOKEN_KEY, "", "/", "PT0S");
              ResponseHeaders headers =
                  ResponseHeaders.builder()
                      .status(HttpStatus.OK)
                      .add(HttpHeaderNames.SET_COOKIE, expiredCookie.toSetCookieHeader())
                      .contentType(MediaType.JSON)
                      .build();
              // Armeria requires a non-empty response payload, so an empty JSON is sent
              return HttpResponse.of(headers, HttpData.ofUtf8(EMPTY_RESPONSE));
            })
        .orElse(HttpResponse.of(HttpStatus.OK, MediaType.JSON, EMPTY_RESPONSE));
  }

  private static void verifyPrincipal(DecodedJWT decodedJWT) {
    String subject =
        decodedJWT
            .getClaims()
            .getOrDefault(JwtClaim.EMAIL.key(), decodedJWT.getClaim(JwtClaim.SUBJECT.key()))
            .asString();

    LOGGER.debug("Validating principal: {}", subject);

    if (subject.equals("admin")) {
      LOGGER.debug("admin always allowed");
      return;
    }

    try {
      User user = USER_REPOSITORY.getUserByEmail(subject);
      if (user != null && user.getState() == User.StateEnum.ENABLED) {
        LOGGER.debug("Principal {} is enabled", subject);
        return;
      }
    } catch (Exception e) {
      // IGNORE
    }

    throw new OAuthInvalidRequestException(
        ErrorCode.INVALID_ARGUMENT, "User not allowed: " + subject);
  }

  private Cookie createCookie(String key, String value, String path, String maxAge) {
    return Cookie.secureBuilder(key, value)
        .path(path)
        .maxAge(Duration.parse(maxAge).getSeconds())
        .build();
  }

  // NOTE:
  // Unfortunately, when specifying `application/x-www-form-urlencoded` for content in the OpenAPI
  // schema, the OpenAPI Generator does not generate request models from that schema.
  // Additionally, when accessing each parameter directly from the body without using a model,
  // Armelia fails to handle them correctly if the `ext` query parameter is present. Therefore,
  // the request model is implemented manually here.
  //
  // SEE:
  // - https://armeria.dev/docs/server-annotated-service/#getting-a-query-parameter
  // - https://armeria.dev/docs/server-annotated-service/#injecting-a-parameter-as-an-enum-type
  @ToString
  private static class OAuthTokenExchangeRequest {
    @Param("grant_type")
    @Getter
    private String grantType;

    @Param("requested_token_type")
    @Getter
    private String requestedTokenType;

    @Param("subject_token_type")
    @Getter
    private String subjectTokenType;

    @Param("subject_token")
    @Getter
    private String subjectToken;

    @Param("actor_token_type")
    @Nullable
    @Getter
    private String actorTokenType;

    @Param("actor_token")
    @Nullable
    @Getter
    private String actorToken;
  }
}
