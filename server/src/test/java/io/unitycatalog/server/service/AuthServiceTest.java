package io.unitycatalog.server.service;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.Cookie;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import io.unitycatalog.server.exception.BaseExceptionHandler;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.AdminTokenCommand;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import io.unitycatalog.server.utils.JwksOperations;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthServiceTest extends BaseAuthCRUDTest {

  private static final String LOGOUT_ENDPOINT = "/api/1.0/unity-control/auth/logout";
  private static final String TOKEN_ENDPOINT = "/api/1.0/unity-control/auth/tokens";
  private static final String ENABLED_USER_EMAIL = "test-user@example.com";
  private static final String EMPTY_RESPONSE = "{}";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  // Okta's SCIM 2.0 requests, from developer.okta.com/docs/api/openapi/okta-scim/guides/scim-20/.
  private static final String OKTA_CREATE_USER =
      """
      {"schemas":["urn:ietf:params:scim:schemas:core:2.0:User"],"userName":"test.user@okta.local",
      "name":{"givenName":"Test","familyName":"User"},
      "emails":[{"primary":true,"value":"test.user@okta.local","type":"work"}],
      "displayName":"Test User","locale":"en-US","externalId":"00ujl29u0le5T6Aj10h7","groups":[],
      "password":"1mz050nq","active":true}\
      """;
  private static final String OKTA_DEACTIVATE_USER =
      """
      {"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
      "Operations":[{"op":"replace","value":{"active":false}}]}\
      """;

  private WebClient client;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    observabilityEnabled = true;
    // BaseServerTest turns stack traces on; use the default (off) to check none leak to callers.
    serverProperties.remove(Property.INCLUDE_STACK_TRACE_IN_ERROR.getKey());
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    client = WebClient.builder(serverConfig.getServerUrl()).build();
  }

  @Test
  public void testLogout() throws Exception {
    // Logout with cookie should return status as 200 and empty ejson content
    RequestHeaders headersWithCookie = buildLogoutRequestHeader(true);

    AggregatedHttpResponse response = client.execute(headersWithCookie).aggregate().join();
    assertEquals(HttpStatus.OK, response.status());
    assertThat(response.contentUtf8()).isEqualTo(EMPTY_RESPONSE);
    TestUtils.assertHttpRequestMetric(
        serverConfig,
        observabilityServerConfig,
        "io.unitycatalog.server.service.AuthService",
        "logout",
        HttpStatus.OK.code(),
        1.0);

    // Logout without cookie should return 401 (no credentials provided)
    RequestHeaders headersWithoutCookie = buildLogoutRequestHeader(false);
    response = client.execute(headersWithoutCookie).aggregate().join();
    assertEquals(HttpStatus.UNAUTHORIZED, response.status());
  }

  @Test
  public void testCookieWritesRequireAjaxHeader() throws Exception {
    for (String requestedWith : List.of("", "fetch")) {
      RequestHeadersBuilder headers =
          RequestHeaders.builder(HttpMethod.POST, "/api/1.0/unity-control/scim2/Users")
              .add(HttpHeaderNames.COOKIE, "UC_TOKEN=" + securityContext.createServiceToken());
      if (!requestedWith.isEmpty()) {
        headers.add(HttpHeaderNames.X_REQUESTED_WITH, requestedWith);
      }
      AggregatedHttpResponse response = client.execute(headers.build()).aggregate().join();
      assertThat(response.status()).isEqualTo(HttpStatus.FORBIDDEN);
      JsonNode error = MAPPER.readTree(response.contentUtf8());
      assertThat(error.get("error_code").asText()).isEqualTo("PERMISSION_DENIED");
      assertThat(error.get("message").asText())
          .isEqualTo("Cookie requests require X-Requested-With: XMLHttpRequest");
      assertThat(response.headers().get(HttpHeaderNames.SET_COOKIE)).isNull();
    }

    RequestHeaders read =
        RequestHeaders.builder(HttpMethod.GET, "/api/1.0/unity-control/scim2/Me")
            .add(HttpHeaderNames.COOKIE, "UC_TOKEN=" + securityContext.createServiceToken())
            .build();
    assertThat(client.execute(read).aggregate().join().status()).isEqualTo(HttpStatus.OK);

    // An explicit bearer token takes precedence over cookies and needs no AJAX header.
    RequestHeaders bearer =
        RequestHeaders.builder(HttpMethod.POST, LOGOUT_ENDPOINT)
            .add(HttpHeaderNames.AUTHORIZATION, "Bearer " + securityContext.createServiceToken())
            .add(HttpHeaderNames.COOKIE, "UC_TOKEN=invalid")
            .build();
    assertThat(client.execute(bearer).aggregate().join().status()).isEqualTo(HttpStatus.OK);

    RequestHeaders preflight =
        RequestHeaders.builder(HttpMethod.OPTIONS, LOGOUT_ENDPOINT)
            .add(HttpHeaderNames.ORIGIN, "https://other.example.com")
            .add(HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .add(HttpHeaderNames.ACCESS_CONTROL_REQUEST_HEADERS, "x-requested-with")
            .build();
    AggregatedHttpResponse response = client.execute(preflight).aggregate().join();
    assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN)).isNull();
    assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS)).isNull();
  }

  @Test
  public void testExpiredAccessTokenIsRejected() throws Exception {
    // Request with expired access token should return 401
    RequestHeaders headers = buildLogoutRequestHeaderWithToken(createExpiredAccessToken());

    AggregatedHttpResponse response = client.execute(headers).aggregate().join();
    assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
    TestUtils.assertHttpRequestMetric(
        serverConfig,
        observabilityServerConfig,
        "io.unitycatalog.server.service.AuthService",
        "logout",
        HttpStatus.UNAUTHORIZED.code(),
        1.0);
  }

  private RequestHeaders buildLogoutRequestHeader(boolean includeCookie) {
    RequestHeadersBuilder builder =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(LOGOUT_ENDPOINT)
            .contentType(MediaType.JSON)
            .add(HttpHeaderNames.X_REQUESTED_WITH, "XMLHttpRequest");

    if (includeCookie) {
      builder.add(HttpHeaderNames.COOKIE, "UC_TOKEN=" + securityContext.createServiceToken());
    }

    return builder.build();
  }

  private RequestHeaders buildLogoutRequestHeaderWithToken(String token) {
    return RequestHeaders.builder()
        .method(HttpMethod.POST)
        .path(LOGOUT_ENDPOINT)
        .contentType(MediaType.JSON)
        .add(HttpHeaderNames.X_REQUESTED_WITH, "XMLHttpRequest")
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

  private AggregatedHttpResponse exchangeToken(String identityToken) {
    return exchangeToken(
        identityToken,
        RequestHeaders.builder(HttpMethod.POST, TOKEN_ENDPOINT)
            .contentType(MediaType.FORM_DATA)
            .build());
  }

  private AggregatedHttpResponse exchangeToken(String identityToken, RequestHeaders headers) {
    String formBody =
        "grant_type=urn:ietf:params:oauth:grant-type:token-exchange"
            + "&requested_token_type=urn:ietf:params:oauth:token-type:access_token"
            + "&subject_token_type=urn:ietf:params:oauth:token-type:id_token"
            + "&subject_token="
            + identityToken;

    return client.execute(headers, HttpData.ofUtf8(formBody)).aggregate().join();
  }

  @Test
  public void testCookieLoginRequiresAjaxHeader() throws Exception {
    createEnabledUser(ENABLED_USER_EMAIL);
    String token =
        createIdentityToken(
            ENABLED_USER_EMAIL, testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);
    RequestHeaders headers =
        RequestHeaders.builder(HttpMethod.POST, TOKEN_ENDPOINT + "?ext=cookie")
            .contentType(MediaType.FORM_DATA)
            .build();
    AggregatedHttpResponse rejected = exchangeToken(token, headers);
    assertThat(rejected.status()).isEqualTo(HttpStatus.FORBIDDEN);
    JsonNode error = MAPPER.readTree(rejected.contentUtf8());
    assertThat(error.get("error_code").asText()).isEqualTo("PERMISSION_DENIED");
    assertThat(error.get("message").asText())
        .isEqualTo("Cookie requests require X-Requested-With: XMLHttpRequest");
    assertThat(rejected.headers().get(HttpHeaderNames.SET_COOKIE)).isNull();

    AggregatedHttpResponse accepted =
        exchangeToken(
            token,
            headers.toBuilder().add(HttpHeaderNames.X_REQUESTED_WITH, "XMLHttpRequest").build());
    assertThat(accepted.status()).isEqualTo(HttpStatus.OK);
    Cookie cookie = Cookie.fromSetCookieHeader(accepted.headers().get(HttpHeaderNames.SET_COOKIE));
    assertThat(cookie.name()).isEqualTo("UC_TOKEN");
    assertThat(cookie.value())
        .isEqualTo(MAPPER.readTree(accepted.contentUtf8()).get("access_token").asText());
    assertThat(cookie.path()).isEqualTo("/");
    assertThat(cookie.maxAge()).isEqualTo(Duration.ofDays(1).getSeconds());
    assertThat(cookie.sameSite()).isEqualTo("Strict");
    assertThat(cookie.isSecure()).isTrue();
    assertThat(cookie.isHttpOnly()).isTrue();
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
    assertThat(
            JWT.decode(body.get("access_token").asText())
                .getClaim(JwtClaim.SUBJECT.key())
                .asString())
        .isEqualTo(ENABLED_USER_EMAIL);
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

  /**
   * Journey for the UC tokens the server accepts: a token from the exchange and the bootstrap
   * SERVICE token (no expiry) work; a token with a missing or unknown type, or an ACCESS token with
   * no expiry, gets 401; disabling the user refuses its still-valid token on the next request.
   */
  @Test
  public void testUcTokenChecksJourney() throws Exception {
    createEnabledUser(ENABLED_USER_EMAIL);
    String identityToken =
        createIdentityToken(
            ENABLED_USER_EMAIL, testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);
    String accessToken =
        MAPPER.readTree(exchangeToken(identityToken).contentUtf8()).get("access_token").asText();
    assertThat(listCatalogs(accessToken).status()).isEqualTo(HttpStatus.OK);
    String serviceToken = securityContext.createServiceToken();
    assertThat(JWT.decode(serviceToken).getExpiresAt()).isNull();
    assertThat(listCatalogs(serviceToken).status()).isEqualTo(HttpStatus.OK);

    String typeMessage =
        "Access token has a missing or unknown type. Get a new token from this server.";
    Date expiresAt = Date.from(Instant.now().plus(Duration.ofMinutes(5)));
    Map<String, String> rejected =
        Map.of(
            ucSignedToken(null, expiresAt), typeMessage,
            ucSignedToken("BOGUS", expiresAt), typeMessage,
            ucSignedToken(JwtTokenType.ACCESS.name(), null),
                "Access token has no expiry. Get a new token from this server.");
    for (Map.Entry<String, String> entry : rejected.entrySet()) {
      AggregatedHttpResponse response = listCatalogs(entry.getKey());
      assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
      JsonNode error = MAPPER.readTree(response.contentUtf8());
      assertThat(error.get("error_code").asText()).isEqualTo("UNAUTHENTICATED");
      assertThat(error.get("message").asText()).isEqualTo(entry.getValue());
    }

    // The admin disables the user; the token it already holds is refused on the next request.
    JsonNode users = MAPPER.readTree(scim(HttpMethod.GET, "", null).contentUtf8());
    ObjectNode user = null;
    for (JsonNode resource : users.get("Resources")) {
      if (resource.get("emails").get(0).get("value").asText().equals(ENABLED_USER_EMAIL)) {
        user = (ObjectNode) resource;
      }
    }
    assertThat(user).isNotNull();
    user.put("active", false);
    String id = user.get("id").asText();
    assertThat(scim(HttpMethod.PUT, "/" + id, user.toString()).status()).isEqualTo(HttpStatus.OK);
    AggregatedHttpResponse refused = listCatalogs(accessToken);
    assertThat(refused.status()).isEqualTo(HttpStatus.FORBIDDEN);
    assertThat(MAPPER.readTree(refused.contentUtf8()).get("message").asText())
        .isEqualTo("User not allowed: " + ENABLED_USER_EMAIL);
  }

  /**
   * An unknown key id on an exchanged IdP token is the caller's error: 401, with no per-request
   * ERROR log and no WARN from the key lookup. It is sent twice so it also hits the rate-limited
   * key refresh. (The server ignores the key id of its own tokens.)
   */
  @Test
  public void testUnknownSigningKeyIdIsUnauthenticated() throws Exception {
    createEnabledUser(ENABLED_USER_EMAIL);
    Logger logger = (Logger) LogManager.getLogger(BaseExceptionHandler.class);
    CapturingAppender appender = new CapturingAppender();
    appender.start();
    logger.addAppender(appender);
    Logger jwksLogger = (Logger) LogManager.getLogger(JwksOperations.class);
    Level previousJwksLevel = jwksLogger.getLevel();
    CapturingAppender jwksAppender = new CapturingAppender();
    jwksAppender.start();
    jwksLogger.addAppender(jwksAppender);
    Configurator.setLevel(jwksLogger.getName(), Level.DEBUG);
    try {
      for (int i = 0; i < 2; i++) {
        String identityToken =
            createIdentityToken(
                ENABLED_USER_EMAIL,
                testIssuer,
                TEST_AUDIENCE,
                testIssuerAlgorithm,
                "unknown-kid-" + i);
        AggregatedHttpResponse response = exchangeToken(identityToken);
        assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
        JsonNode error = MAPPER.readTree(response.contentUtf8());
        assertThat(error.get("error_code").asText()).isEqualTo("UNAUTHENTICATED");
        assertThat(error.get("message").asText()).isEqualTo("Unknown signing key");
      }
    } finally {
      logger.removeAppender(appender);
      appender.stop();
      jwksLogger.removeAppender(jwksAppender);
      Configurator.setLevel(jwksLogger.getName(), previousJwksLevel);
      jwksAppender.stop();
    }
    assertThat(appender.messages).doesNotContain("Request failed");
    assertThat(jwksAppender.messages).anyMatch(m -> m.startsWith("No usable signing key"));
    assertThat(jwksAppender.levels).allMatch(level -> level.isLessSpecificThan(Level.INFO));
  }

  /** A token signed with the server's key for the enabled user; null leaves the claim out. */
  private String ucSignedToken(String type, Date expiresAt) {
    var builder =
        JWT.create()
            .withIssuer(INTERNAL)
            .withExpiresAt(expiresAt)
            .withClaim(JwtClaim.SUBJECT.key(), ENABLED_USER_EMAIL);
    if (type != null) {
      builder.withClaim(JwtClaim.TOKEN_TYPE.key(), type);
    }
    return builder.sign(securityContext.getAlgorithm());
  }

  private AggregatedHttpResponse listCatalogs(String token) {
    return client
        .execute(
            RequestHeaders.builder(HttpMethod.GET, "/api/2.1/unity-catalog/catalogs")
                .add(HttpHeaderNames.AUTHORIZATION, "Bearer " + token)
                .build())
        .aggregate()
        .join();
  }

  /**
   * Journey for an IdP's SCIM token: the admin creates the IdP principal, mints its token with
   * {@code bin/uc-admin-token}, and the IdP provisions a user with Okta's request. The token is
   * refused off the SCIM API, the principal can't sign in, and disabling it revokes the token.
   */
  @Test
  public void testIdpScimTokenJourney() throws Exception {
    // 1. The admin creates the IdP principal.
    AggregatedHttpResponse created =
        scim(
            HttpMethod.POST,
            "",
            "{\"displayName\":\"Okta"
                + " SCIM\",\"emails\":[{\"value\":\"okta-scim\",\"primary\":true}]}");
    assertThat(created.status()).isEqualTo(HttpStatus.CREATED);
    String principalId = MAPPER.readTree(created.contentUtf8()).get("id").asText();

    // 2. The admin mints its SCIM token, as bin/uc-admin-token does.
    String scimToken =
        AdminTokenCommand.createScimToken(
            new String[] {"--scim-principal", "okta-scim", "--idp", "okta", "--lifetime", "P90D"},
            securityContext.getAlgorithm(),
            new UserRepository(null, hibernateConfigurator.getSessionFactory()));
    DecodedJWT claims = JWT.decode(scimToken);
    assertThat(claims.getClaim(JwtClaim.TOKEN_TYPE.key()).asString()).isEqualTo("SCIM");
    assertThat(claims.getSubject()).isEqualTo(principalId);
    assertThat(claims.getClaim(JwtClaim.IDP.key()).asString()).isEqualTo("okta");
    assertThat(claims.getExpiresAt().toInstant())
        .isCloseTo(Instant.now().plus(Duration.ofDays(90)), within(1, ChronoUnit.MINUTES));

    // 3. Okta checks for the user, then creates it (Okta's SCIM 2.0 requests).
    AggregatedHttpResponse lookup =
        scimAs(
            scimToken,
            HttpMethod.GET,
            "?filter=userName%20eq%20%22test.user%40okta.local%22&startIndex=1&count=100",
            null);
    assertThat(lookup.status()).isEqualTo(HttpStatus.OK);
    assertThat(MAPPER.readTree(lookup.contentUtf8()).get("totalResults").asInt()).isZero();
    AggregatedHttpResponse oktaCreate = scimAs(scimToken, HttpMethod.POST, "", OKTA_CREATE_USER);
    assertThat(oktaCreate.status()).isEqualTo(HttpStatus.CREATED);
    assertThat(MAPPER.readTree(oktaCreate.contentUtf8()).get("emails").get(0).get("value").asText())
        .isEqualTo("test.user@okta.local");

    // 3b. With the SCIM token, not metastore OWNER, read the user, update it with Okta's PUT, then
    // deactivate it with Okta's PATCH and delete it (Okta itself never sends DELETE).
    String userId = MAPPER.readTree(oktaCreate.contentUtf8()).get("id").asText();
    assertThat(scimAs(scimToken, HttpMethod.GET, "/" + userId, null).status())
        .isEqualTo(HttpStatus.OK);
    AggregatedHttpResponse renamed =
        scimAs(
            scimToken,
            HttpMethod.PUT,
            "/" + userId,
            OKTA_CREATE_USER
                .replace("\"userName\"", "\"id\":\"" + userId + "\",\"userName\"")
                .replace("Test User", "Renamed User"));
    assertThat(renamed.status()).isEqualTo(HttpStatus.OK);
    assertThat(MAPPER.readTree(renamed.contentUtf8()).get("displayName").asText())
        .isEqualTo("Renamed User");
    assertThat(scimAs(scimToken, HttpMethod.PATCH, "/" + userId, OKTA_DEACTIVATE_USER).status())
        .isEqualTo(HttpStatus.OK);
    assertThat(scimAs(scimToken, HttpMethod.DELETE, "/" + userId, null).status())
        .isEqualTo(HttpStatus.OK);

    // 4. The token is refused off the SCIM API.
    AggregatedHttpResponse catalogs = listCatalogs(scimToken);
    assertThat(catalogs.status()).isEqualTo(HttpStatus.FORBIDDEN);
    assertThat(MAPPER.readTree(catalogs.contentUtf8()).get("message").asText())
        .isEqualTo("A SCIM token is accepted only under /api/1.0/unity-control/scim2/.");

    // 5. The IdP principal can't sign in.
    assertExchangeRejectedAsInvalid(
        "email=okta-scim",
        createIdentityToken(
            "okta-scim", testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId));

    // 6. The admin disables the principal (Okta's deactivate request); its token stops working.
    assertThat(scim(HttpMethod.PATCH, "/" + principalId, OKTA_DEACTIVATE_USER).status())
        .isEqualTo(HttpStatus.OK);
    AggregatedHttpResponse disabled = scimAs(scimToken, HttpMethod.GET, "", null);
    assertThat(disabled.status()).isEqualTo(HttpStatus.FORBIDDEN);
    assertThat(MAPPER.readTree(disabled.contentUtf8()).get("message").asText())
        .isEqualTo("User not allowed: " + principalId);
  }

  /** {@code bin/uc-admin-token} refuses SCIM options it can't mint a token from. */
  @Test
  public void testScimTokenMintRefusals() {
    UserRepository users = new UserRepository(null, hibernateConfigurator.getSessionFactory());
    String usage =
        "Usage: bin/uc-admin-token [--scim-principal <email> --idp <name> --lifetime <ISO-8601"
            + " duration>]";
    Map<List<String>, String> refusals =
        Map.of(
            List.of("--scim-principal", "okta-scim", "--idp", "okta"),
            "--lifetime is required with --scim-principal. " + usage,
            List.of("--scim-principal", "okta-scim", "--lifetime", "P90D"),
            "--idp is required with --scim-principal. " + usage,
            List.of("--idp", "okta", "--lifetime", "P90D"),
            "--scim-principal is required with --idp and --lifetime. " + usage,
            List.of("--scim-principal", "okta-scim", "--idp", "okta", "--lifetime", "90 days"),
            "--lifetime must be a positive ISO-8601 duration, such as P90D: 90 days",
            List.of("--scim-principal", "okta-scim", "--idp", "okta", "--lifetime", "-P1D"),
            "--lifetime must be a positive ISO-8601 duration, such as P90D: -P1D",
            List.of("--scim-principal", "okta-scim", "--idp"),
            "--idp needs a value. " + usage,
            List.of("--kid", "x"),
            "Unknown option: --kid. " + usage);
    refusals.forEach(
        (args, message) ->
            assertThatThrownBy(
                    () ->
                        AdminTokenCommand.createScimToken(
                            args.toArray(String[]::new), securityContext.getAlgorithm(), users))
                .as(String.join(" ", args))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(message));

    // The lifetime is checked before the user is marked: empty, zero, too short, or an expiry out
    // of range.
    createEnabledUser("okta-scim");
    Map.of(
            "",
            "--lifetime needs a value. " + usage,
            "PT0S",
            "--lifetime must be a positive ISO-8601 duration, such as P90D: PT0S",
            "PT0.5S",
            "--lifetime must be at least 1 second: PT0.5S",
            "PT9223372036854775807S",
            "--lifetime is too long: PT9223372036854775807S",
            "P200000000000D",
            "--lifetime is too long: P200000000000D")
        .forEach(
            (lifetime, message) ->
                assertThatThrownBy(
                        () ->
                            AdminTokenCommand.createScimToken(
                                new String[] {
                                  "--scim-principal",
                                  "okta-scim",
                                  "--idp",
                                  "okta",
                                  "--lifetime",
                                  lifetime
                                },
                                securityContext.getAlgorithm(),
                                users))
                    .as(lifetime)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(message));
    assertThat(users.getUserByEmail("okta-scim").getIdpName()).isNull();
  }

  /**
   * A SCIM token can't PUT, PATCH or DELETE an IdP principal, its own or another IdP's, so one IdP
   * can't lock out or revive another; the admin still can.
   */
  @Test
  public void testScimTokenCannotChangeIdpPrincipals() throws Exception {
    UserRepository users = new UserRepository(null, hibernateConfigurator.getSessionFactory());
    createEnabledUser("okta-scim");
    createEnabledUser("entra-scim");
    String okta = mintScimToken(users, "okta-scim", "okta");
    String entra = mintScimToken(users, "entra-scim", "entra");
    String oktaId = JWT.decode(okta).getSubject();
    String entraId = JWT.decode(entra).getSubject();
    String put =
        "{\"schemas\":[\"urn:ietf:params:scim:schemas:core:2.0:User\"],\"id\":\"%s\","
            + "\"userName\":\"%s\",\"emails\":[{\"primary\":true,\"value\":\"%s\"}],"
            + "\"displayName\":\"Renamed\",\"active\":false}";
    String reactivate = OKTA_DEACTIVATE_USER.replace("false", "true");
    Map<String, String> principalIds = Map.of("okta-scim", oktaId, "entra-scim", entraId);
    for (String token : List.of(okta, entra)) {
      for (Map.Entry<String, String> principal : principalIds.entrySet()) {
        String email = principal.getKey();
        String id = principal.getValue();
        for (AggregatedHttpResponse response :
            List.of(
                scimAs(token, HttpMethod.PUT, "/" + id, String.format(put, id, email, email)),
                scimAs(token, HttpMethod.PATCH, "/" + id, OKTA_DEACTIVATE_USER),
                scimAs(token, HttpMethod.DELETE, "/" + id, null))) {
          assertThat(response.status()).as(email).isEqualTo(HttpStatus.FORBIDDEN);
          assertThat(MAPPER.readTree(response.contentUtf8()).get("message").asText())
              .isEqualTo("An IdP principal can't be changed with a SCIM token: " + email);
        }
      }
    }
    assertThat(users.getUserByEmail("entra-scim").getState()).isEqualTo(User.StateEnum.ENABLED);
    assertThat(users.getUserByEmail("okta-scim").getName()).isNotEqualTo("Renamed");

    // The admin disables Okta's principal; Entra's token can't revive it.
    assertThat(scim(HttpMethod.PATCH, "/" + oktaId, OKTA_DEACTIVATE_USER).status())
        .isEqualTo(HttpStatus.OK);
    assertThat(scimAs(entra, HttpMethod.PATCH, "/" + oktaId, reactivate).status())
        .isEqualTo(HttpStatus.FORBIDDEN);
    assertThat(scimAs(okta, HttpMethod.GET, "", null).status()).isEqualTo(HttpStatus.FORBIDDEN);
    assertThat(scimAs(entra, HttpMethod.GET, "", null).status()).isEqualTo(HttpStatus.OK);
  }

  /**
   * A SCIM token can't relink an existing user: it can't change the external id that token exchange
   * resolves an OAuth client to, so it can't take over the user. Okta's own PUT, which resends the
   * external id it set, still works.
   */
  @Test
  public void testScimTokenCannotRelinkAnExistingUser() throws Exception {
    UserRepository users = new UserRepository(null, hibernateConfigurator.getSessionFactory());
    createEnabledUser("victim@example.com");
    String victimId = users.getUserByEmail("victim@example.com").getId();
    createEnabledUser("okta-scim");
    String okta = mintScimToken(users, "okta-scim", "okta");

    // Okta creates a user, then sends its full-object PUT with the same values: 200.
    AggregatedHttpResponse oktaCreate = scimAs(okta, HttpMethod.POST, "", OKTA_CREATE_USER);
    String oktaUserId = MAPPER.readTree(oktaCreate.contentUtf8()).get("id").asText();
    String oktaPut =
        OKTA_CREATE_USER.replace("\"userName\"", "\"id\":\"" + oktaUserId + "\",\"userName\"");
    assertThat(scimAs(okta, HttpMethod.PUT, "/" + oktaUserId, oktaPut).status())
        .isEqualTo(HttpStatus.OK);

    // A PUT that changes the external id is refused, on any existing user.
    String put =
        "{\"schemas\":[\"urn:ietf:params:scim:schemas:core:2.0:User\"],\"id\":\"%s\","
            + "\"userName\":\"%s\",\"emails\":[{\"primary\":true,\"value\":\"%s\"}],"
            + "\"displayName\":\"Test User\",\"active\":true,\"externalId\":\"attacker-client\"}";
    for (String email : List.of("victim@example.com", "test.user@okta.local")) {
      String id = users.getUserByEmail(email).getId();
      AggregatedHttpResponse response =
          scimAs(okta, HttpMethod.PUT, "/" + id, String.format(put, id, email, email));
      assertThat(response.status()).as(email).isEqualTo(HttpStatus.FORBIDDEN);
      assertThat(MAPPER.readTree(response.contentUtf8()).get("message").asText())
          .isEqualTo("A SCIM token can't change the external id of a user: " + email);
    }
    // A PUT that changes the primary email is refused, a case variant included: token exchange
    // signs in a user by email. So is one with no primary email, or a primary with no value.
    String emailPut = put.replace(",\"externalId\":\"attacker-client\"", "");
    String storedEmails = "[{\"primary\":true,\"value\":\"victim@example.com\"}]";
    for (String emails :
        List.of(
            "[{\"primary\":true,\"value\":\"attacker@example.com\"}]",
            "[{\"primary\":true,\"value\":\"Victim@example.com\"}]",
            "[{\"primary\":false,\"value\":\"victim@example.com\"},{\"value\":\"x@example.com\"}]",
            "[{\"primary\":true}]",
            "[null]",
            "[null,{\"primary\":true,\"value\":\"attacker@example.com\"}]")) {
      String body =
          String.format(emailPut, victimId, "victim@example.com", "victim@example.com")
              .replace(storedEmails, emails);
      AggregatedHttpResponse response = scimAs(okta, HttpMethod.PUT, "/" + victimId, body);
      assertThat(response.status()).as(emails).isEqualTo(HttpStatus.FORBIDDEN);
      assertThat(MAPPER.readTree(response.contentUtf8()).get("message").asText())
          .isEqualTo("A SCIM token can't change the email of a user: victim@example.com");
    }
    // A PUT that resends the stored email as the primary email, or sends no emails, is accepted.
    for (String body :
        List.of(
            String.format(emailPut, victimId, "victim@example.com", "victim@example.com"),
            String.format(emailPut, victimId, "victim@example.com", "victim@example.com")
                .replace(storedEmails, "[null," + storedEmails.substring(1)),
            String.format(emailPut, victimId, "victim@example.com", "victim@example.com")
                .replace(
                    "\"emails\":[{\"primary\":true,\"value\":\"victim@example.com\"}],", ""))) {
      assertThat(scimAs(okta, HttpMethod.PUT, "/" + victimId, body).status())
          .as(body)
          .isEqualTo(HttpStatus.OK);
    }
    // PATCH changes only active.
    for (String patch :
        List.of(
            "{\"op\":\"replace\",\"value\":{\"externalId\":\"attacker-client\"}}",
            "{\"op\":\"replace\",\"path\":\"externalId\",\"value\":\"attacker-client\"}",
            "{\"op\":\"replace\",\"path\":\"emails\",\"value\":[{\"primary\":true,"
                + "\"value\":\"attacker@example.com\"}]}")) {
      scimAs(
          okta,
          HttpMethod.PATCH,
          "/" + victimId,
          "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],\"Operations\":["
              + patch
              + "]}");
    }
    User victim = users.getUser(victimId);
    assertThat(victim.getExternalId()).isNull();
    assertThat(victim.getEmail()).isEqualTo("victim@example.com");
    assertThat(users.getUser(oktaUserId).getExternalId()).isEqualTo("00ujl29u0le5T6Aj10h7");

    // So an access token for the attacker's OAuth client resolves to no user.
    String accessToken =
        JWT.create()
            .withSubject("opaque-subject")
            .withIssuer(testIssuer)
            .withAudience(TEST_AUDIENCE)
            .withIssuedAt(new Date())
            .withKeyId(testIssuerKeyId)
            .withClaim("azp", "attacker-client")
            .sign(testIssuerAlgorithm);
    AggregatedHttpResponse exchange =
        client
            .execute(
                RequestHeaders.builder(HttpMethod.POST, TOKEN_ENDPOINT)
                    .contentType(MediaType.FORM_DATA)
                    .build(),
                HttpData.ofUtf8(
                    "grant_type=urn:ietf:params:oauth:grant-type:token-exchange"
                        + "&requested_token_type=urn:ietf:params:oauth:token-type:access_token"
                        + "&subject_token_type=urn:ietf:params:oauth:token-type:access_token"
                        + "&subject_token="
                        + accessToken))
            .aggregate()
            .join();
    assertThat(exchange.status()).isEqualTo(HttpStatus.BAD_REQUEST);
  }

  private String mintScimToken(UserRepository users, String email, String idp) {
    return AdminTokenCommand.createScimToken(
        new String[] {"--scim-principal", email, "--idp", idp, "--lifetime", "P1D"},
        securityContext.getAlgorithm(),
        users);
  }

  /** Calls the SCIM users endpoint as the admin. */
  private AggregatedHttpResponse scim(HttpMethod method, String path, String body) {
    return scimAs(securityContext.createServiceToken(), method, path, body);
  }

  /** Calls the SCIM users endpoint with {@code token}, as Okta does. */
  private AggregatedHttpResponse scimAs(String token, HttpMethod method, String path, String body) {
    RequestHeaders headers =
        RequestHeaders.builder(method, "/api/1.0/unity-control/scim2/Users" + path)
            .contentType(MediaType.parse("application/scim+json"))
            .add(HttpHeaderNames.AUTHORIZATION, "Bearer " + token)
            .build();
    return client
        .execute(headers, body == null ? HttpData.empty() : HttpData.ofUtf8(body))
        .aggregate()
        .join();
  }

  @Test
  public void testTokenExchangeDebugLogOmitsSubjectToken() {
    createEnabledUser(ENABLED_USER_EMAIL);
    String token =
        createIdentityToken(
            ENABLED_USER_EMAIL, testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);
    Logger logger = (Logger) LogManager.getLogger(AuthService.class);
    Level previousLevel = logger.getLevel();
    CapturingAppender appender = new CapturingAppender();
    appender.start();
    logger.addAppender(appender);
    Configurator.setLevel(logger.getName(), Level.DEBUG);
    try {
      assertThat(exchangeToken(token).status()).isEqualTo(HttpStatus.OK);
    } finally {
      logger.removeAppender(appender);
      Configurator.setLevel(logger.getName(), previousLevel);
      appender.stop();
    }
    assertThat(appender.messages)
        .contains(
            "Token exchange request: grant_type=urn:ietf:params:oauth:grant-type:token-exchange,"
                + " requested_token_type=urn:ietf:params:oauth:token-type:access_token,"
                + " subject_token_type=urn:ietf:params:oauth:token-type:id_token");
    assertThat(String.join("\n", appender.messages)).doesNotContain(token);
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
  public void testTokenExchangeRejectsDisallowedPrincipals() throws Exception {
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
    TestUtils.assertHttpRequestMetric(
        serverConfig,
        observabilityServerConfig,
        "io.unitycatalog.server.service.AuthService",
        "grantToken",
        HttpStatus.BAD_REQUEST.code(),
        4.0);
  }

  private void assertExchangeRejectedAsInvalid(String description, String token)
      throws IOException {
    AggregatedHttpResponse response = exchangeToken(token);
    assertThat(response.status()).as(description).isEqualTo(HttpStatus.BAD_REQUEST);
    JsonNode error = MAPPER.readTree(response.contentUtf8());
    assertThat(error.get("error_code").asText()).as(description).isEqualTo("INVALID_ARGUMENT");
    assertThat(error.has("access_token")).as(description).isFalse();
    assertThat(error.has("stack_trace")).as(description).isFalse();
  }

  @Test
  public void testScimErrorsReturnTheirStatusAsValidJson() throws Exception {
    String scimUsersPath = "/api/1.0/unity-control/scim2/Users";
    String userJson =
        "{\"displayName\":\"test\",\"emails\":[{\"value\":\"scim-dup@test.com\",\"primary\":true}]}";
    RequestHeaders headers =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(scimUsersPath)
            .contentType(MediaType.JSON)
            .add(HttpHeaderNames.X_REQUESTED_WITH, "XMLHttpRequest")
            .add(HttpHeaderNames.COOKIE, "UC_TOKEN=" + securityContext.createServiceToken())
            .build();

    // First create succeeds
    AggregatedHttpResponse first =
        client.execute(headers, HttpData.ofUtf8(userJson)).aggregate().join();
    assertThat(first.status().code()).isEqualTo(201);
    TestUtils.assertHttpRequestMetric(
        serverConfig,
        observabilityServerConfig,
        "io.unitycatalog.server.service.Scim2UserService",
        "createScimUser",
        HttpStatus.CREATED.code(),
        1.0);

    // Second create triggers Scim2RuntimeException wrapping ResourceConflictException
    AggregatedHttpResponse second =
        client.execute(headers, HttpData.ofUtf8(userJson)).aggregate().join();
    assertThat(second.status()).isEqualTo(HttpStatus.CONFLICT);
    TestUtils.assertHttpRequestMetric(
        serverConfig,
        observabilityServerConfig,
        "io.unitycatalog.server.service.Scim2UserService",
        "createScimUser",
        HttpStatus.CONFLICT.code(),
        1.0);

    // Verify the SCIM error response is a valid JSON object (not double-serialized)
    // with the expected SCIM error fields
    String body = second.contentUtf8();
    JsonNode json = MAPPER.readTree(body);
    assertThat(json.isObject()).as("Expected JSON object but got: " + body).isTrue();
    assertThat(json.has("schemas")).isTrue();
    assertThat(json.get("status").asText()).isEqualTo("409");
    assertThat(json.has("detail")).isTrue();
    assertThat(json.get("detail").asText()).contains("already exists");

    // A different SCIM error keeps its own status rather than collapsing to 409
    String noPrimaryEmailJson =
        "{\"displayName\":\"test\",\"emails\":[{\"value\":\"scim-np@test.com\",\"primary\":false}]}";
    AggregatedHttpResponse noPrimary =
        client.execute(headers, HttpData.ofUtf8(noPrimaryEmailJson)).aggregate().join();
    assertThat(noPrimary.status()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
    assertThat(MAPPER.readTree(noPrimary.contentUtf8()).get("status").asText()).isEqualTo("412");
  }

  @Test
  public void testScimCreateUserDatabaseFailureIsAServerError() throws Exception {
    // The database rejects inserting this one email, so the create fails inside its transaction.
    String failingEmail = "scim-db-failure@test.com";
    runSql(
        "ALTER TABLE uc_users ADD CONSTRAINT scim_db_failure CHECK (email <> '"
            + failingEmail
            + "')");
    try {
      RequestHeaders headers =
          RequestHeaders.builder()
              .method(HttpMethod.POST)
              .path("/api/1.0/unity-control/scim2/Users")
              .contentType(MediaType.JSON)
              .add(HttpHeaderNames.COOKIE, "UC_TOKEN=" + securityContext.createServiceToken())
              .add(HttpHeaderNames.X_REQUESTED_WITH, "XMLHttpRequest")
              .build();
      String userJson =
          "{\"displayName\":\"test\",\"emails\":[{\"value\":\""
              + failingEmail
              + "\",\"primary\":true}]}";

      AggregatedHttpResponse response =
          client.execute(headers, HttpData.ofUtf8(userJson)).aggregate().join();

      assertThat(response.status()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
      JsonNode json = MAPPER.readTree(response.contentUtf8());
      assertThat(json.get("status").asText()).isEqualTo("500");
      assertThat(json.get("detail").asText()).startsWith("Failed to create user");
    } finally {
      runSql("ALTER TABLE uc_users DROP CONSTRAINT scim_db_failure");
    }
  }

  private void runSql(String sql) {
    hibernateConfigurator
        .getSessionFactory()
        .inTransaction(session -> session.createNativeMutationQuery(sql).executeUpdate());
  }

  private static final class CapturingAppender extends AbstractAppender {

    private final List<String> messages = new CopyOnWriteArrayList<>();
    private final List<Level> levels = new CopyOnWriteArrayList<>();

    private CapturingAppender() {
      super(
          "AuthServiceTest",
          null,
          null,
          true,
          org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      messages.add(event.getMessage().getFormattedMessage());
      levels.add(event.getLevel());
    }
  }
}
