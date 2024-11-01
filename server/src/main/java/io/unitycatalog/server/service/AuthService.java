package io.unitycatalog.server.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.linecorp.armeria.common.*;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
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
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExceptionHandler(GlobalExceptionHandler.class)
public class AuthService {

  // TODO: need common module for these constants, they are reused in the CLI
  interface Fields {
    String GRANT_TYPE = "grant_type";
    String CLIENT_ID = "client_id";
    String CLIENT_SECRET = "client_secret";
    String SUBJECT_TOKEN = "subject_token";
    String SUBJECT_TOKEN_TYPE = "subject_token_type";
    String ACTOR_TOKEN = "actor_token";
    String ACTOR_TOKEN_TYPE = "actor_token_type";
    String REQUESTED_TOKEN_TYPE = "requested_token_type";
  }

  interface GrantTypes {
    String TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange";
  }

  interface TokenTypes {
    String ACCESS = "urn:ietf:params:oauth:token-type:access_token";
    String ID = "urn:ietf:params:oauth:token-type:id_token";
    String JWT = "urn:ietf:params:oauth:token-type:jwt";
  }

  interface AuthTypes {
    String BEARER = "Bearer";
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthService.class);
  private static final UserRepository USER_REPOSITORY = UserRepository.getInstance();

  private final SecurityContext securityContext;
  private final JwksOperations jwksOperations;

  private static final String COOKIE = "cookie";

  public AuthService(SecurityContext securityContext) {
    this.securityContext = securityContext;
    this.jwksOperations = new JwksOperations();
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
   * <p>TODO: This could probably be integrated into the OpenAPI spec.
   *
   * @param request The token exchange parameters
   * @return The token exchange response
   */
  @Post("/tokens")
  public HttpResponse grantToken(
      @Param("ext") Optional<String> ext, OAuthTokenExchangeRequest request) {
    LOGGER.debug("Got token: {}", request);
    if (request.getGrantType() == null
        || !GrantTypes.TOKEN_EXCHANGE.equals(request.getGrantType())) {
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT, "Unsupported grant type: " + request.getGrantType());
    }

    if (request.getRequestedTokenType() == null
        || !TokenTypes.ACCESS.equals(request.getRequestedTokenType())) {
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
    String issuer = decodedJWT.getClaim("iss").asString();
    String keyId = decodedJWT.getHeaderClaim("kid").asString();
    LOGGER.debug("Validating token for issuer: {}", issuer);

    JWTVerifier jwtVerifier = jwksOperations.verifierForIssuerAndKey(issuer, keyId);
    decodedJWT = jwtVerifier.verify(decodedJWT);
    verifyPrincipal(decodedJWT);

    LOGGER.debug("Validated. Creating access token.");

    String accessToken = securityContext.createAccessToken(decodedJWT);

    OAuthTokenExchangeResponse response =
        OAuthTokenExchangeResponse.builder()
            .accessToken(accessToken)
            .issuedTokenType(TokenTypes.ACCESS)
            .tokenType(AuthTypes.BEARER)
            .build();

    // Set token as cookie if ext param is set to cookie
    ResponseHeadersBuilder responseHeaders = ResponseHeaders.builder(HttpStatus.OK);
    ext.ifPresent(
        e -> {
          if (e.equals(COOKIE)) {
            // Set cookie timeout to 5 days by default if not present in server.properties
            String cookieTimeout =
                ServerProperties.getInstance().getProperty("server.cookie-timeout", "P5D");
            Cookie cookie =
                createCookie(
                    AuthDecorator.UC_TOKEN_KEY,
                    accessToken,
                    "/",
                    Duration.parse(cookieTimeout).getSeconds());
            responseHeaders.add(HttpHeaderNames.SET_COOKIE, cookie.toSetCookieHeader());
          }
        });

    return HttpResponse.ofJson(responseHeaders.build(), response);
  }

  @Get("/logout")
  public HttpResponse logout(HttpRequest request) {
    String authorizationCookie =
        request.headers().cookies().stream()
            .filter(c -> c.name().equals(AuthDecorator.UC_TOKEN_KEY))
            .map(Cookie::name)
            .findFirst()
            .orElse(null);

    if (authorizationCookie != null) {
      Cookie expiredCookie = createCookie(AuthDecorator.UC_TOKEN_KEY, "", "/", 0L);
      ResponseHeaders headers =
          ResponseHeaders.of(
              HttpStatus.OK, HttpHeaderNames.SET_COOKIE, expiredCookie.toSetCookieHeader());
      return HttpResponse.of(headers);
    }
    return HttpResponse.of(HttpStatus.OK);
  }

  private static void verifyPrincipal(DecodedJWT decodedJWT) {
    String subject =
        decodedJWT.getClaim(JwtClaim.EMAIL.key()).isMissing()
            ? decodedJWT.getClaim(JwtClaim.SUBJECT.key()).asString()
            : decodedJWT.getClaim(JwtClaim.EMAIL.key()).asString();

    if (subject.equals("admin")) {
      return;
    }

    try {
      User user = USER_REPOSITORY.getUserByEmail(subject);
      if (user != null && user.getState() == User.StateEnum.ENABLED) {
        return;
      }
    } catch (Exception e) {
      // IGNORE
    }

    throw new OAuthInvalidRequestException(
        ErrorCode.INVALID_ARGUMENT, "User not allowed: " + subject);
  }

  private Cookie createCookie(String key, String value, String path, Long maxAge) {
    return Cookie.secureBuilder(key, value).path(path).maxAge(maxAge).build();
  }

  // TODO: This should be probably integrated into the OpenAPI spec.
  @ToString
  static class OAuthTokenExchangeRequest {
    @Param(Fields.GRANT_TYPE)
    @Getter
    private String grantType;

    @Param(Fields.REQUESTED_TOKEN_TYPE)
    @Getter
    private String requestedTokenType;

    @Param(Fields.SUBJECT_TOKEN_TYPE)
    @Getter
    private String subjectTokenType;

    @Param(Fields.SUBJECT_TOKEN)
    @Getter
    private String subjectToken;

    @Param(Fields.ACTOR_TOKEN_TYPE)
    @Nullable
    @Getter
    private String actorTokenType;

    @Param(Fields.ACTOR_TOKEN)
    @Nullable
    @Getter
    private String actorToken;
  }

  // TODO: This should be probably integrated into the OpenAPI spec.
  @Jacksonized
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Builder
  @Getter
  public static class OAuthTokenExchangeResponse {
    @NonNull private String accessToken;
    @NonNull private String issuedTokenType;
    @NonNull private String tokenType;
    private Long expiresIn;
    private String scope;
    private String refreshToken;
  }
}
