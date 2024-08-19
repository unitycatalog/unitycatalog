package io.unitycatalog.server.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.JwksOperations;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExceptionHandler(GlobalExceptionHandler.class)
public class AuthService {

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

  private final SecurityContext securityContext;
  private final JwksOperations jwksOperations;

  public AuthService(SecurityContext securityContext) {
    this.securityContext = securityContext;
    this.jwksOperations = new JwksOperations();
  }

  /**
   * OAuth token exchange.
   *
   * <p>Performs an OAuth token exchange for an access-token.
   *
   * <p>TODO: This could probably be integrated into the OpenAPI spec.
   *
   * @param request The token exchange parameters
   * @return The token exchange response
   */
  @Post("/tokens")
  public HttpResponse grantToken(OAuthTokenExchangeRequest request) {
    LOGGER.debug("Got token: {}", request);

    if (request.getGrantType() != null
        && !TokenTypes.ACCESS.equals(request.getRequestedTokenType())) {
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

    DecodedJWT decodedJWT = JWT.decode(request.getSubjectToken());
    String issuer = decodedJWT.getClaim("iss").asString();
    String keyId = decodedJWT.getHeaderClaim("kid").asString();

    LOGGER.debug("Validating token for issuer: {}", issuer);

    JWTVerifier jwtVerifier = jwksOperations.verifierForIssuerAndKey(issuer, keyId);
    decodedJWT = jwtVerifier.verify(decodedJWT);

    String accessToken = securityContext.createAccessToken(decodedJWT);

    OAuthTokenExchangeResponse response =
        OAuthTokenExchangeResponse.builder()
            .accessToken(accessToken)
            .issuedTokenType(TokenTypes.ACCESS)
            .tokenType(AuthTypes.BEARER)
            .build();

    return HttpResponse.ofJson(response);
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
    @Getter
    private String actorTokenType;

    @Param(Fields.ACTOR_TOKEN)
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
