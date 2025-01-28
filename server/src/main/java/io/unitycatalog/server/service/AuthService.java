package io.unitycatalog.server.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.*;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.QueryParams;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.RequestConverter;
import com.linecorp.armeria.server.annotation.RequestConverterFunction;
import io.unitycatalog.control.model.AccessTokenType;
import io.unitycatalog.control.model.GrantType;
import io.unitycatalog.control.model.OAuthTokenExchangeForm;
import io.unitycatalog.control.model.OAuthTokenExchangeInfo;
import io.unitycatalog.control.model.TokenEndpointExtensionType;
import io.unitycatalog.control.model.TokenType;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.JwksOperations;
import io.unitycatalog.server.utils.ServerProperties;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExceptionHandler(GlobalExceptionHandler.class)
public class AuthService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthService.class);
  private final UserRepository userRepository;

  private final SecurityContext securityContext;
  private final JwksOperations jwksOperations;
  private final ServerProperties serverProperties;

  private static final String EMPTY_RESPONSE = "{}";

  public AuthService(
      SecurityContext securityContext,
      ServerProperties serverProperties,
      Repositories repositories) {
    this.securityContext = securityContext;
    this.jwksOperations = new JwksOperations(securityContext);
    this.serverProperties = serverProperties;
    this.userRepository = repositories.getUserRepository();
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
   * @param request The token exchange parameters
   * @return The token exchange response
   */
  @Post("/tokens")
  public HttpResponse grantToken(
      @Param("ext") Optional<TokenEndpointExtensionType> ext,
      @RequestConverter(ToOAuthTokenExchangeFormConverter.class) OAuthTokenExchangeForm form) {
    LOGGER.debug("Got token: {}", form);

    if (GrantType.TOKEN_EXCHANGE != form.getGrantType()) {
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT, "Unsupported grant type: " + form.getGrantType());
    }

    if (TokenType.ACCESS_TOKEN != form.getRequestedTokenType()) {
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT,
          "Unsupported requested token type: " + form.getRequestedTokenType());
    }

    if (form.getSubjectTokenType() == null) {
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT, "Subject token type is required but was not specified");
    }

    if (form.getActorTokenType() != null) {
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT, "Actor tokens not currently supported");
    }

    boolean authorizationEnabled = this.serverProperties.isAuthorizationEnabled();
    if (!authorizationEnabled) {
      throw new OAuthInvalidRequestException(
          ErrorCode.INVALID_ARGUMENT, "Authorization is disabled");
    }

    DecodedJWT decodedJWT = JWT.decode(form.getSubjectToken());
    String issuer = decodedJWT.getIssuer();
    String keyId = decodedJWT.getKeyId();

    LOGGER.debug("Validating token for issuer: {} and keyId: {}", issuer, keyId);

    JWTVerifier jwtVerifier = jwksOperations.verifierForIssuerAndKey(issuer, keyId);
    decodedJWT = jwtVerifier.verify(decodedJWT);
    verifyPrincipal(decodedJWT);

    LOGGER.debug("Validated. Creating access token.");

    String accessToken = securityContext.createAccessToken(decodedJWT);

    OAuthTokenExchangeInfo tokenExchangeInfo =
        new OAuthTokenExchangeInfo()
            .accessToken(accessToken)
            .issuedTokenType(TokenType.ACCESS_TOKEN)
            .tokenType(AccessTokenType.BEARER);

    // Set token as cookie if ext param is set to cookie
    ResponseHeadersBuilder responseHeaders = ResponseHeaders.builder(HttpStatus.OK);
    ext.ifPresent(
        e -> {
          if (e.equals(TokenEndpointExtensionType.COOKIE)) {
            // Set cookie timeout to 5 days by default if not present in server.properties
            String cookieTimeout =
                this.serverProperties.getProperty("server.cookie-timeout", "P5D");
            Cookie cookie =
                createCookie(AuthDecorator.UC_TOKEN_KEY, accessToken, "/", cookieTimeout);
            responseHeaders.add(HttpHeaderNames.SET_COOKIE, cookie.toSetCookieHeader());
          }
        });

    return HttpResponse.ofJson(responseHeaders.build(), tokenExchangeInfo);
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

  private void verifyPrincipal(DecodedJWT decodedJWT) {
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
      User user = userRepository.getUserByEmail(subject);
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
  // When specifying `application/x-www-form-urlencoded` as the content type in the OpenAPI schema,
  // the OpenAPI Generator does not create request models from the schema.
  // Moreover, directly accessing parameters from the body without a model causes issues with
  // Armeria, particularly when the `ext` query parameter is included.
  //
  // To resolve this, instead of redefining a request model solely for Armeria's parameter
  // injection,
  // a `RequestConverterFunction` for `OAuthTokenExchangeRequest` is implemented here.
  // This approach ensures a single model is used across both the `controlApi` and `cli` projects,
  // preserving the principle of a single source of truth.
  //
  // SEE:
  // - https://armeria.dev/docs/server-annotated-service/#getting-a-query-parameter
  // - https://armeria.dev/docs/server-annotated-service/#injecting-a-parameter-as-an-enum-type
  private static class ToOAuthTokenExchangeFormConverter implements RequestConverterFunction {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Object convertRequest(
        ServiceRequestContext ctx,
        AggregatedHttpRequest request,
        Class<?> expectedResultType,
        @Nullable ParameterizedType expectedParameterizedResultType) {
      MediaType contentType = request.contentType();
      if (expectedResultType == OAuthTokenExchangeForm.class
          && contentType != null
          && contentType.belongsTo(MediaType.FORM_DATA)) {
        Map<String, String> form =
            QueryParams.fromQueryString(
                    request.content(contentType.charset(StandardCharsets.UTF_8)))
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return mapper.convertValue(form, OAuthTokenExchangeForm.class);
      }
      return RequestConverterFunction.fallthrough();
    }
  }
}
