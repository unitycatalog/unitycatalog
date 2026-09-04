package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.unitycatalog.control.model.TokenType;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.OAuthInvalidRequestException;
import io.unitycatalog.server.persist.UserRepository;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TokenExchangeSubjectTokenHandlerTest {

  private static final String CLIENT_ID = "6e8bc60c-5e4d-4cd8-93ce-0b7c02b32499";
  private static final String SERVICE_EMAIL = "service@example.com";
  private static final String USER_EMAIL = "human@example.com";

  @Mock private UserRepository userRepository;

  private TokenExchangeSubjectTokenHandler handler;

  @BeforeEach
  void setUp() {
    handler = new TokenExchangeSubjectTokenHandler(userRepository);
  }

  @Test
  void hasEmailClaimDetectsMissingEmailClaim() {
    DecodedJWT jwt = decode(tokenWithAzpAndAud(CLIENT_ID, "https://dev.dev.example.com"));

    assertThat(TokenExchangeSubjectTokenHandler.hasEmailClaim(jwt)).isFalse();
  }

  @Test
  void extractOAuthClientIdPrefersAzp() {
    DecodedJWT jwt = decode(tokenWithAzpAndAud(CLIENT_ID, "https://dev.dev.example.com"));

    assertThat(TokenExchangeSubjectTokenHandler.extractOAuthClientId(jwt)).contains(CLIENT_ID);
  }

  @Test
  void extractOAuthClientIdReadsClientIdClaim() {
    DecodedJWT jwt = decode(tokenWithClientIdClaim(CLIENT_ID));

    assertThat(TokenExchangeSubjectTokenHandler.extractOAuthClientId(jwt)).contains(CLIENT_ID);
  }

  @Test
  void extractOAuthClientIdIgnoresAudienceOnlyTokens() {
    DecodedJWT jwt = decode(tokenWithAudOnly(CLIENT_ID, "https://dev.dev.example.com"));

    assertThat(TokenExchangeSubjectTokenHandler.extractOAuthClientId(jwt)).isEmpty();
  }

  @Test
  void idTokenDoesNotResolveViaOauthClientId() {
    DecodedJWT jwt = decode(identityToken(USER_EMAIL, CLIENT_ID));
    when(userRepository.getUserByEmail(USER_EMAIL))
        .thenThrow(new BaseException(ErrorCode.NOT_FOUND, "User not found"));

    assertThatThrownBy(() -> handler.resolvePrincipalEmail(TokenType.ID_TOKEN, jwt))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessageContaining("User not allowed");
    verify(userRepository, never()).getUserByExternalId(CLIENT_ID);
  }

  @Test
  void accessTokenResolvesViaOauthClientId() {
    DecodedJWT jwt = decode(tokenWithAzpAndAud(CLIENT_ID, "https://dev.dev.example.com"));
    when(userRepository.getUserByExternalId(CLIENT_ID)).thenReturn(enabledUser(SERVICE_EMAIL));

    assertThat(handler.resolvePrincipalEmail(TokenType.ACCESS_TOKEN, jwt)).isEqualTo(SERVICE_EMAIL);
  }

  @Test
  void missingUserIsAuthDenialNotServerError() {
    DecodedJWT jwt = decode(identityToken(USER_EMAIL, CLIENT_ID));
    when(userRepository.getUserByEmail(USER_EMAIL))
        .thenThrow(new BaseException(ErrorCode.NOT_FOUND, "User not found"));

    assertThatThrownBy(() -> handler.resolvePrincipalEmail(TokenType.ID_TOKEN, jwt))
        .isInstanceOf(OAuthInvalidRequestException.class)
        .hasMessageContaining("User not allowed");
  }

  @Test
  void nonNotFoundBaseExceptionOnEmailLookupIsNotAuthDenial() {
    DecodedJWT jwt = decode(identityToken(USER_EMAIL, CLIENT_ID));
    when(userRepository.getUserByEmail(USER_EMAIL))
        .thenThrow(new BaseException(ErrorCode.INTERNAL, "database unavailable"));

    assertThatThrownBy(() -> handler.resolvePrincipalEmail(TokenType.ID_TOKEN, jwt))
        .isInstanceOf(BaseException.class)
        .hasMessage("database unavailable");
  }

  @Test
  void repositoryFailureOnEmailLookupIsNotAuthDenial() {
    DecodedJWT jwt = decode(identityToken(USER_EMAIL, CLIENT_ID));
    when(userRepository.getUserByEmail(USER_EMAIL)).thenThrow(new RuntimeException("db down"));

    assertThatThrownBy(() -> handler.resolvePrincipalEmail(TokenType.ID_TOKEN, jwt))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("db down");
  }

  @Test
  void repositoryFailureOnExternalIdLookupIsNotAuthDenial() {
    DecodedJWT jwt = decode(tokenWithAzpAndAud(CLIENT_ID, "https://dev.dev.example.com"));
    when(userRepository.getUserByExternalId(CLIENT_ID)).thenThrow(new RuntimeException("db down"));

    assertThatThrownBy(() -> handler.resolvePrincipalEmail(TokenType.ACCESS_TOKEN, jwt))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("db down");
  }

  private static User enabledUser(String email) {
    return new User().email(email).state(User.StateEnum.ENABLED);
  }

  private static DecodedJWT decode(String token) {
    return JWT.decode(token);
  }

  private static String identityToken(String email, String clientId) {
    return JWT.create()
        .withSubject("subject")
        .withClaim("email", email)
        .withClaim("azp", clientId)
        .withAudience("unity-catalog")
        .withIssuer("https://issuer.example.com")
        .withIssuedAt(new Date())
        .withJWTId(UUID.randomUUID().toString())
        .sign(Algorithm.none());
  }

  private static String tokenWithAzpAndAud(String clientId, String realmAudience) {
    return JWT.create()
        .withSubject("subject")
        .withClaim("azp", clientId)
        .withAudience(clientId, realmAudience)
        .withIssuer("https://issuer.example.com")
        .withIssuedAt(new Date())
        .withJWTId(UUID.randomUUID().toString())
        .sign(Algorithm.none());
  }

  private static String tokenWithAudOnly(String... audiences) {
    return JWT.create()
        .withSubject("subject")
        .withAudience(audiences)
        .withIssuer("https://issuer.example.com")
        .withIssuedAt(new Date())
        .withJWTId(UUID.randomUUID().toString())
        .sign(Algorithm.none());
  }

  private static String tokenWithClientIdClaim(String clientId) {
    return JWT.create()
        .withSubject("subject")
        .withClaim("client_id", clientId)
        .withIssuer("https://issuer.example.com")
        .withIssuedAt(new Date())
        .withJWTId(UUID.randomUUID().toString())
        .sign(Algorithm.none());
  }
}
