package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import io.unitycatalog.server.security.JwtClaim;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Token exchange tests for email-first and subject-token externalId principal resolution. */
public class AuthServiceClientPrincipalTest extends BaseAuthCRUDTest {

  private static final String TOKEN_ENDPOINT = "/api/1.0/unity-control/auth/tokens";
  private static final String SCIM_USERS_PATH = "/api/1.0/unity-control/scim2/Users";
  private static final String OAUTH_CLIENT_ID = "oauth-client-uuid";
  private static final String SERVICE_EMAIL = "storium-uc-test@example.com";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private WebClient client;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    client = WebClient.builder(serverConfig.getServerUrl()).build();
    createServiceUser();
  }

  @Test
  public void testIdTokenExchangeResolvesPrincipalByEmailBeforeExternalId() throws IOException {
    String token =
        createIdentityToken(
            testIssuer, OAUTH_CLIENT_ID, SERVICE_EMAIL, testIssuerAlgorithm, testIssuerKeyId);

    AggregatedHttpResponse response =
        exchangeToken(token, "urn:ietf:params:oauth:token-type:id_token");

    assertThat(response.status()).isEqualTo(HttpStatus.OK);
    JsonNode body = MAPPER.readTree(response.contentUtf8());
    assertIssuedUcTokenSubject(body, SERVICE_EMAIL);
  }

  @Test
  public void testIdTokenExchangeResolvesPrincipalByExternalId() throws IOException {
    String token =
        createIdentityToken(
            testIssuer,
            OAUTH_CLIENT_ID,
            "wrong-email@example.com",
            testIssuerAlgorithm,
            testIssuerKeyId);

    AggregatedHttpResponse response =
        exchangeToken(token, "urn:ietf:params:oauth:token-type:id_token");

    assertThat(response.status()).isEqualTo(HttpStatus.OK);
    JsonNode body = MAPPER.readTree(response.contentUtf8());
    assertIssuedUcTokenSubject(body, SERVICE_EMAIL);
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
            "unknown-client-id",
            "unknown-email@example.com",
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

  private static void assertIssuedUcTokenSubject(JsonNode body, String expectedSubject) {
    assertThat(body.has("access_token")).isTrue();
    String accessToken = body.get("access_token").asText();
    String subject = JWT.decode(accessToken).getClaim(JwtClaim.SUBJECT.key()).asString();
    assertThat(subject).isEqualTo(expectedSubject);
  }

  private void createServiceUser() {
    String userJson =
        "{"
            + "\"displayName\":\"Storium UC test\","
            + "\"externalId\":\""
            + OAUTH_CLIENT_ID
            + "\","
            + "\"emails\":[{\"value\":\""
            + SERVICE_EMAIL
            + "\",\"primary\":true}]"
            + "}";
    RequestHeaders headers =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(SCIM_USERS_PATH)
            .contentType(MediaType.JSON)
            .add(HttpHeaderNames.COOKIE, "UC_TOKEN=" + securityContext.getServiceToken())
            .build();
    AggregatedHttpResponse response =
        client.execute(headers, HttpData.ofUtf8(userJson)).aggregate().join();
    assertThat(response.status().code()).isEqualTo(201);
  }

  private static String createIdentityToken(
      String issuer, String audience, String email, Algorithm algorithm, String keyId) {
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
    return builder.sign(algorithm);
  }

  private static String createAccessTokenSubject(
      String issuer, String clientId, Algorithm algorithm, String keyId) {
    return JWT.create()
        .withSubject("subject-uuid")
        .withClaim("azp", clientId)
        .withAudience(clientId, "https://dev.dev.example.com")
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
        .withAudience(clientId, "https://dev.dev.example.com")
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
