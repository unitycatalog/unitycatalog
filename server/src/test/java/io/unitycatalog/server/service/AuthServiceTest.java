package io.unitycatalog.server.service;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
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
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import java.io.IOException;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthServiceTest extends BaseAuthCRUDTest {

  private static final String LOGOUT_ENDPOINT = "/api/1.0/unity-control/auth/logout";
  private static final String TOKEN_ENDPOINT = "/api/1.0/unity-control/auth/tokens";
  private static final String SCIM_USERS_ENDPOINT = "/api/1.0/unity-control/scim2/Users";
  private static final String ENABLED_USER_EMAIL = "test-user@example.com";
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

  @Test
  public void testExpiredAccessTokenIsRejected() {
    // Request with expired access token should return 401
    RequestHeaders headers = buildLogoutRequestHeaderWithToken(createExpiredAccessToken());

    AggregatedHttpResponse response = client.execute(headers).aggregate().join();
    assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
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

  private RequestHeaders buildLogoutRequestHeaderWithToken(String token) {
    return RequestHeaders.builder()
        .method(HttpMethod.POST)
        .path(LOGOUT_ENDPOINT)
        .contentType(MediaType.JSON)
        .add(HttpHeaderNames.COOKIE, "UC_TOKEN=" + token)
        .build();
  }

  private String createExpiredAccessToken() {
    Date issuedAt = new Date(System.currentTimeMillis() - Duration.ofHours(2).toMillis());
    Date expiresAt = new Date(System.currentTimeMillis() - Duration.ofHours(1).toMillis());

    return JWT.create()
        .withSubject(securityContext.getServiceName())
        .withIssuer(INTERNAL)
        .withIssuedAt(issuedAt)
        .withExpiresAt(expiresAt)
        .withKeyId(securityContext.getKeyId())
        .withJWTId(UUID.randomUUID().toString())
        .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.ACCESS.name())
        .withClaim(JwtClaim.SUBJECT.key(), "admin")
        .sign(securityContext.getAlgorithm());
  }

  /**
   * Creates a signed identity token.
   *
   * @param subject the principal asserted by the token (used as the {@code sub} claim)
   * @param issuer the token issuer
   * @param audience the token audience (may be null)
   * @param algorithm the signing algorithm
   * @param keyId the key ID for the JWT header
   * @return signed JWT string
   */
  private String createIdentityToken(
      String subject, String issuer, String audience, Algorithm algorithm, String keyId) {
    var builder =
        JWT.create()
            .withSubject(subject)
            .withIssuer(issuer)
            .withIssuedAt(new Date())
            .withKeyId(keyId)
            .withJWTId(UUID.randomUUID().toString());
    if (audience != null) {
      builder.withAudience(audience);
    }
    return builder.sign(algorithm);
  }

  /** Creates an ENABLED user via the SCIM endpoint using the internal service token. */
  private void createEnabledUser(String email) {
    String userJson =
        String.format(
            "{\"displayName\":\"Test User\",\"emails\":[{\"value\":\"%s\",\"primary\":true}]}",
            email);
    RequestHeaders headers =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(SCIM_USERS_ENDPOINT)
            .contentType(MediaType.JSON)
            .add(HttpHeaderNames.COOKIE, "UC_TOKEN=" + securityContext.getServiceToken())
            .build();
    AggregatedHttpResponse response =
        client.execute(headers, HttpData.ofUtf8(userJson)).aggregate().join();
    assertThat(response.status().code()).isEqualTo(201);
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

  /**
   * Also covers the token endpoint's interaction with the authorization gate: it is excluded from
   * the access decorators, so nothing sets a PayloadAuthorizer, and it binds its body with its own
   * {@code @RequestConverter} rather than the gate. Both together are why it still succeeds.
   */
  @Test
  public void testTokenExchangeWithCorrectIssuerAndAudience() throws IOException {
    createEnabledUser(ENABLED_USER_EMAIL);
    String token =
        createIdentityToken(
            ENABLED_USER_EMAIL, testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

    AggregatedHttpResponse response = exchangeToken(token);

    assertThat(response.status()).isEqualTo(HttpStatus.OK);

    JsonNode body = MAPPER.readTree(response.contentUtf8());
    assertThat(body.has("access_token")).isTrue();
    assertThat(body.get("access_token").asText()).isNotEmpty();
    assertThat(body.get("issued_token_type").asText())
        .isEqualTo("urn:ietf:params:oauth:token-type:access_token");
    assertThat(body.get("token_type").asText()).isEqualTo("Bearer");

    Duration expectedTtl = Duration.parse("PT24H");
    assertThat(body.get("expires_in").asLong()).isEqualTo(expectedTtl.getSeconds());

    DecodedJWT accessJwt = JWT.decode(body.get("access_token").asText());
    assertThat(accessJwt.getIssuedAt()).isNotNull();
    assertThat(accessJwt.getExpiresAt()).isNotNull();
    assertThat(accessJwt.getExpiresAt().toInstant())
        .isCloseTo(
            accessJwt.getIssuedAt().toInstant().plus(expectedTtl), within(2, ChronoUnit.SECONDS));
  }

  @Test
  public void testTokenExchangeWithCorrectIssuerAndWrongAudience() {
    String token =
        createIdentityToken(
            ENABLED_USER_EMAIL, testIssuer, "wrong-audience", testIssuerAlgorithm, testIssuerKeyId);

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
            ENABLED_USER_EMAIL,
            "https://evil-issuer.example.com",
            TEST_AUDIENCE,
            foreignAlgorithm,
            foreignKeyId);

    AggregatedHttpResponse response = exchangeToken(token);

    // The issuer is not in the allowlist → 403 Forbidden
    assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
  }

  @Test
  public void testTokenExchangeRejectsDisallowedPrincipals() throws IOException {
    // The reserved "admin" principal (the internal service-token, metastore-OWNER identity) must
    // never be exchangeable, including a case variant such as "ADMIN" that a case-insensitive
    // database collation would resolve back to the admin user; and a correctly-signed token for
    // any non-enabled user must fail closed. All return the same generic INVALID_ARGUMENT.
    for (String subject : List.of("admin", "ADMIN", "nobody@example.com")) {
      assertExchangeRejectedAsInvalid(
          "sub=" + subject,
          createIdentityToken(
              subject, testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId));
    }

    // The principal is taken from the "email" claim (falling back to "sub"), so an opaque subject
    // paired with email=admin must be rejected just like sub=admin.
    String emailAdminToken =
        JWT.create()
            .withSubject("opaque-subject-id")
            .withIssuer(testIssuer)
            .withAudience(TEST_AUDIENCE)
            .withIssuedAt(new Date())
            .withKeyId(testIssuerKeyId)
            .withJWTId(UUID.randomUUID().toString())
            .withClaim(JwtClaim.EMAIL.key(), "admin")
            .sign(testIssuerAlgorithm);
    assertExchangeRejectedAsInvalid("email=admin", emailAdminToken);
  }

  private void assertExchangeRejectedAsInvalid(String description, String token)
      throws IOException {
    AggregatedHttpResponse response = exchangeToken(token);
    assertThat(response.status()).as(description).isEqualTo(HttpStatus.BAD_REQUEST);
    JsonNode error = MAPPER.readTree(response.contentUtf8());
    assertThat(error.get("error_code").asText()).as(description).isEqualTo("INVALID_ARGUMENT");
    assertThat(error.has("access_token")).as(description).isFalse();
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
    assertThat(second.status()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);

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
