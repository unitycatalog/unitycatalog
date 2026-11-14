package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import io.unitycatalog.server.security.JwtClaim;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Token exchange tests for email-first and subject-token externalId principal resolution. */
public class AuthServiceClientPrincipalTest extends BaseAuthCRUDTest {

  private static final String TOKEN_ENDPOINT = "/api/1.0/unity-control/auth/tokens";
  private static final String OAUTH_CLIENT_ID = "oauth-client-uuid";
  private static final String SERVICE_EMAIL = "storium-uc-test@example.com";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private WebClient client;
  private HttpServer idpTokenServer;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    client = WebClient.builder(serverConfig.getServerUrl()).build();
    createEnabledUser(SERVICE_EMAIL, OAUTH_CLIENT_ID);
  }

  @AfterEach
  @Override
  public void tearDown() {
    if (idpTokenServer != null) {
      idpTokenServer.stop(0);
    }
    super.tearDown();
  }

  @Test
  public void testIdTokenExchangeResolvesPrincipalByEmailBeforeExternalId() throws IOException {
    String token =
        createIdentityToken(
            testIssuer,
            TEST_AUDIENCE,
            OAUTH_CLIENT_ID,
            SERVICE_EMAIL,
            testIssuerAlgorithm,
            testIssuerKeyId);

    AggregatedHttpResponse response =
        exchangeToken(token, "urn:ietf:params:oauth:token-type:id_token");

    assertThat(response.status()).isEqualTo(HttpStatus.OK);
    JsonNode body = MAPPER.readTree(response.contentUtf8());
    assertIssuedUcTokenSubject(body, SERVICE_EMAIL);
  }

  @Test
  public void testIdTokenExchangeDoesNotFallBackToExternalId() {
    String token =
        createIdentityToken(
            testIssuer,
            TEST_AUDIENCE,
            OAUTH_CLIENT_ID,
            "wrong-email@example.com",
            testIssuerAlgorithm,
            testIssuerKeyId);

    AggregatedHttpResponse response =
        exchangeToken(token, "urn:ietf:params:oauth:token-type:id_token");

    assertThat(response.status()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(response.contentUtf8()).contains("User not allowed");
  }

  @Test
  public void testAccessTokenExchangeResolvesPrincipalByExternalId() throws IOException {
    String token =
        createAccessTokenSubject(testIssuer, OAUTH_CLIENT_ID, testIssuerAlgorithm, testIssuerKeyId);

    AggregatedHttpResponse response =
        exchangeToken(token, "urn:ietf:params:oauth:token-type:access_token");

    assertThat(response.status()).isEqualTo(HttpStatus.OK);
    JsonNode body = MAPPER.readTree(response.contentUtf8());
    assertIssuedUcTokenSubject(body, SERVICE_EMAIL);
  }

  @Test
  public void testAccessTokenExchangePrefersExternalIdWhenEmailClaimMissing() throws IOException {
    String token =
        createAccessTokenSubjectWithoutEmail(
            testIssuer,
            OAUTH_CLIENT_ID,
            "wrong-email@example.com",
            testIssuerAlgorithm,
            testIssuerKeyId);

    AggregatedHttpResponse response =
        exchangeToken(token, "urn:ietf:params:oauth:token-type:access_token");

    assertThat(response.status()).isEqualTo(HttpStatus.OK);
    JsonNode body = MAPPER.readTree(response.contentUtf8());
    assertIssuedUcTokenSubject(body, SERVICE_EMAIL);
  }

  @Test
  public void testSubjectTokenRejectsUnknownExternalId() {
    String token =
        createIdentityToken(
            testIssuer,
            TEST_AUDIENCE,
            "unknown-client-id",
            "unknown-email@example.com",
            testIssuerAlgorithm,
            testIssuerKeyId);

    AggregatedHttpResponse response =
        exchangeToken(token, "urn:ietf:params:oauth:token-type:id_token");

    assertThat(response.status()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(response.contentUtf8()).contains("User not allowed");
  }

  @Test
  public void testSubjectTokenRejectsMismatchedAudienceEvenWhenExternalIdMatches() {
    String token =
        createIdentityToken(
            testIssuer,
            "wrong-audience",
            OAUTH_CLIENT_ID,
            SERVICE_EMAIL,
            testIssuerAlgorithm,
            testIssuerKeyId);

    AggregatedHttpResponse response =
        exchangeToken(token, "urn:ietf:params:oauth:token-type:id_token");

    assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(response.contentUtf8()).contains("Invalid audience");
  }

  @Test
  public void testSubjectTokenRejectsAudienceOutsideAllowlistWithoutClientId() {
    String token =
        createRealmOnlyAccessToken(
            testIssuer, "https://unknown.example.com", testIssuerAlgorithm, testIssuerKeyId);

    AggregatedHttpResponse response =
        exchangeToken(token, "urn:ietf:params:oauth:token-type:access_token");

    assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(response.contentUtf8()).contains("Invalid audience");
  }

  @Test
  public void testIdentityProviderTokenIsRejectedOnApiCall() {
    // type=oauth sends the identity provider's token as is, and the server accepts only its own.
    TokenProvider provider =
        TokenProvider.create(
            Map.of(
                "type",
                "oauth",
                "oauth.uri",
                startIdpTokenServer(),
                "oauth.clientId",
                OAUTH_CLIENT_ID,
                "oauth.clientSecret",
                "client-secret"));
    CatalogsApi catalogsApi =
        new CatalogsApi(
            ApiClientBuilder.create()
                .uri(serverConfig.getServerUrl())
                .tokenProvider(provider)
                .retryPolicy(JitterDelayRetryPolicy.builder().maxAttempts(1).build())
                .build());

    assertThatThrownBy(() -> catalogsApi.listCatalogs(null, null, /* includeBrowse= */ false))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("403")
        .hasMessageContaining("Invalid access token.");
  }

  /** Serves client credentials tokens signed by the test issuer the server trusts. */
  private String startIdpTokenServer() {
    try {
      idpTokenServer = HttpServer.create(new InetSocketAddress(0), 0);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start identity provider token server", e);
    }
    idpTokenServer.createContext(
        "/token",
        exchange -> {
          String accessToken =
              JWT.create()
                  .withClaim("azp", OAUTH_CLIENT_ID)
                  .withAudience(TEST_AUDIENCE)
                  .withIssuer(testIssuer)
                  .withIssuedAt(new Date())
                  .withExpiresAt(new Date(System.currentTimeMillis() + 3_600_000))
                  .withKeyId(testIssuerKeyId)
                  .withJWTId(UUID.randomUUID().toString())
                  .sign(testIssuerAlgorithm);
          byte[] body =
              String.format(
                      "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":3600}",
                      accessToken)
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    idpTokenServer.start();
    return "http://localhost:" + idpTokenServer.getAddress().getPort() + "/token";
  }

  private static void assertIssuedUcTokenSubject(JsonNode body, String expectedSubject) {
    assertThat(body.has("access_token")).isTrue();
    String accessToken = body.get("access_token").asText();
    String subject = JWT.decode(accessToken).getClaim(JwtClaim.SUBJECT.key()).asString();
    assertThat(subject).isEqualTo(expectedSubject);
  }

  private static String createIdentityToken(
      String issuer,
      String audience,
      String clientId,
      String email,
      Algorithm algorithm,
      String keyId) {
    var builder =
        JWT.create()
            .withSubject("subject-uuid")
            .withClaim("email", email)
            .withIssuer(issuer)
            .withIssuedAt(new Date())
            .withKeyId(keyId)
            .withJWTId(UUID.randomUUID().toString());
    if (audience != null) {
      builder.withAudience(audience);
    }
    if (clientId != null) {
      builder.withClaim("azp", clientId);
    }
    return builder.sign(algorithm);
  }

  private static String createAccessTokenSubject(
      String issuer, String clientId, Algorithm algorithm, String keyId) {
    return JWT.create()
        .withSubject("subject-uuid")
        .withClaim("azp", clientId)
        .withAudience(TEST_AUDIENCE, "https://dev.dev.example.com")
        .withIssuer(issuer)
        .withIssuedAt(new Date())
        .withKeyId(keyId)
        .withJWTId(UUID.randomUUID().toString())
        .sign(algorithm);
  }

  private static String createAccessTokenSubjectWithoutEmail(
      String issuer, String clientId, String subject, Algorithm algorithm, String keyId) {
    return JWT.create()
        .withSubject(subject)
        .withClaim("azp", clientId)
        .withAudience(TEST_AUDIENCE, "https://dev.dev.example.com")
        .withIssuer(issuer)
        .withIssuedAt(new Date())
        .withKeyId(keyId)
        .withJWTId(UUID.randomUUID().toString())
        .sign(algorithm);
  }

  private static String createRealmOnlyAccessToken(
      String issuer, String realmAudience, Algorithm algorithm, String keyId) {
    return JWT.create()
        .withSubject("subject-uuid")
        .withAudience(realmAudience)
        .withIssuer(issuer)
        .withIssuedAt(new Date())
        .withKeyId(keyId)
        .withJWTId(UUID.randomUUID().toString())
        .sign(algorithm);
  }

  private AggregatedHttpResponse exchangeToken(String subjectToken, String subjectTokenType) {
    String formBody =
        "grant_type=urn:ietf:params:oauth:grant-type:token-exchange"
            + "&requested_token_type=urn:ietf:params:oauth:token-type:access_token"
            + "&subject_token_type="
            + subjectTokenType
            + "&subject_token="
            + subjectToken;

    RequestHeaders headers =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(TOKEN_ENDPOINT)
            .contentType(MediaType.FORM_DATA)
            .build();

    return client.execute(headers, HttpData.ofUtf8(formBody)).aggregate().join();
  }
}
