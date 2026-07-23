package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import com.linecorp.armeria.common.RequestHeadersBuilder;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import java.io.IOException;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthServiceTest extends BaseAuthCRUDTest {

  private static final String LOGOUT_ENDPOINT = "/api/1.0/unity-control/auth/logout";
  private static final String TOKEN_ENDPOINT = "/api/1.0/unity-control/auth/tokens";
  private static final String EMPTY_RESPONSE = "{}";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private WebClient client;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    client = WebClient.builder(serverConfig.getServerUrl()).build();
  }

  @Test
  public void testLogout() {
    // Logout with cookie should return status as 200 and empty ejson content
    RequestHeaders headersWithCookie = buildLogoutRequestHeader(true);

    AggregatedHttpResponse response = client.execute(headersWithCookie).aggregate().join();
    assertEquals(HttpStatus.OK, response.status());
    assertThat(response.contentUtf8()).isEqualTo(EMPTY_RESPONSE);

    // Logout without cookie should return 401 (no credentials provided)
    RequestHeaders headersWithoutCookie = buildLogoutRequestHeader(false);
    response = client.execute(headersWithoutCookie).aggregate().join();
    assertEquals(HttpStatus.UNAUTHORIZED, response.status());
  }

  private RequestHeaders buildLogoutRequestHeader(boolean includeCookie) {
    RequestHeadersBuilder builder =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(LOGOUT_ENDPOINT)
            .contentType(MediaType.JSON);

    if (includeCookie) {
      builder.add(HttpHeaderNames.COOKIE, "UC_TOKEN=" + securityContext.getServiceToken());
    }

    return builder.build();
  }

  /**
   * Creates a signed identity token.
   *
   * @param issuer the token issuer
   * @param audience the token audience (may be null)
   * @param algorithm the signing algorithm
   * @param keyId the key ID for the JWT header
   * @return signed JWT string
   */
  private String createIdentityToken(
      String issuer, String audience, Algorithm algorithm, String keyId) {
    var builder =
        JWT.create()
            .withSubject("admin")
            .withIssuer(issuer)
            .withIssuedAt(new Date())
            .withKeyId(keyId)
            .withJWTId(UUID.randomUUID().toString());
    if (audience != null) {
      builder.withAudience(audience);
    }
    return builder.sign(algorithm);
  }

  private AggregatedHttpResponse exchangeToken(String identityToken) {
    String formBody =
        "grant_type=urn:ietf:params:oauth:grant-type:token-exchange"
            + "&requested_token_type=urn:ietf:params:oauth:token-type:access_token"
            + "&subject_token_type=urn:ietf:params:oauth:token-type:id_token"
            + "&subject_token="
            + identityToken;

    RequestHeaders headers =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(TOKEN_ENDPOINT)
            .contentType(MediaType.FORM_DATA)
            .build();

    return client.execute(headers, HttpData.ofUtf8(formBody)).aggregate().join();
  }

  @Test
  public void testTokenExchangeWithCorrectIssuerAndAudience() throws IOException {
    String token =
        createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

    AggregatedHttpResponse response = exchangeToken(token);

    assertThat(response.status()).isEqualTo(HttpStatus.OK);

    JsonNode body = MAPPER.readTree(response.contentUtf8());
    assertThat(body.has("access_token")).isTrue();
    assertThat(body.get("access_token").asText()).isNotEmpty();
    assertThat(body.get("issued_token_type").asText())
        .isEqualTo("urn:ietf:params:oauth:token-type:access_token");
    assertThat(body.get("token_type").asText()).isEqualTo("Bearer");
  }

  @Test
  public void testTokenExchangeWithCorrectIssuerAndWrongAudience() {
    String token =
        createIdentityToken(testIssuer, "wrong-audience", testIssuerAlgorithm, testIssuerKeyId);

    AggregatedHttpResponse response = exchangeToken(token);

    // The JWT verifier rejects the audience claim → 401 Unauthorized
    assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
  }

  @Test
  public void testTokenExchangeWithWrongIssuerAndCorrectAudience() throws NoSuchAlgorithmException {
    // Generate a separate RSA keypair to simulate a foreign identity provider
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    var foreignKeyPair = keyPairGenerator.generateKeyPair();
    Algorithm foreignAlgorithm =
        Algorithm.RSA512(
            (RSAPublicKey) foreignKeyPair.getPublic(), (RSAPrivateKey) foreignKeyPair.getPrivate());
    String foreignKeyId = UUID.randomUUID().toString();

    String token =
        createIdentityToken(
            "https://evil-issuer.example.com", TEST_AUDIENCE, foreignAlgorithm, foreignKeyId);

    AggregatedHttpResponse response = exchangeToken(token);

    // The issuer is not in the allowlist → 403 Forbidden
    assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
  }

  @Test
  public void testScimDuplicateUserReturnsValidJson() throws Exception {
    String scimUsersPath = "/api/1.0/unity-control/scim2/Users";
    String userJson =
        "{\"displayName\":\"test\",\"emails\":[{\"value\":\"scim-dup@test.com\",\"primary\":true}]}";
    RequestHeaders headers =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(scimUsersPath)
            .contentType(MediaType.JSON)
            .add(HttpHeaderNames.COOKIE, "UC_TOKEN=" + securityContext.getServiceToken())
            .build();

    // First create succeeds
    AggregatedHttpResponse first =
        client.execute(headers, HttpData.ofUtf8(userJson)).aggregate().join();
    assertThat(first.status().code()).isEqualTo(201);

    // Second create triggers Scim2RuntimeException wrapping ResourceConflictException
    AggregatedHttpResponse second =
        client.execute(headers, HttpData.ofUtf8(userJson)).aggregate().join();
    assertThat(second.status()).isEqualTo(HttpStatus.CONFLICT);

    // Verify the SCIM error response is a valid JSON object (not double-serialized)
    // with the expected SCIM error fields
    String body = second.contentUtf8();
    JsonNode json = MAPPER.readTree(body);
    assertThat(json.isObject()).as("Expected JSON object but got: " + body).isTrue();
    assertThat(json.has("schemas")).isTrue();
    assertThat(json.get("status").asText()).isEqualTo("409");
    assertThat(json.has("detail")).isTrue();
    assertThat(json.get("detail").asText()).contains("already exists");
  }
}
