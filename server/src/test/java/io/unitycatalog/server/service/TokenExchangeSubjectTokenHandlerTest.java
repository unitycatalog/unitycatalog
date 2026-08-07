package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TokenExchangeSubjectTokenHandlerTest {

  private static final String CLIENT_ID = "6e8bc60c-5e4d-4cd8-93ce-0b7c02b32499";

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

  private static DecodedJWT decode(String token) {
    return JWT.decode(token);
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
