package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import io.unitycatalog.server.base.auth.KeycloakIdentityProvider;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.AdminTokenCommand;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.WildcardAllowlist;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.UnaryOperator;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Integration tests for issuer/audience allowlist matching during token exchange. */
public class AuthServiceAllowlistTest {

  private static final String TOKEN_ENDPOINT = "/api/1.0/unity-control/auth/tokens";
  private static final String ENABLED_USER_EMAIL = "test-user@example.com";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  abstract static class TokenExchangeTestBase extends BaseAuthCRUDTest {

    protected WebClient client;

    @BeforeEach
    @Override
    public void setUp() {
      super.setUp();
      client = WebClient.builder(serverConfig.getServerUrl()).build();
      createEnabledUser(ENABLED_USER_EMAIL);
    }

    protected String createIdentityToken(
        String issuer, String audience, Algorithm algorithm, String keyId) {
      return createIdentityTokenForSubject(issuer, audience, ENABLED_USER_EMAIL, algorithm, keyId);
    }

    protected String createIdentityTokenForSubject(
        String issuer, String audience, String subject, Algorithm algorithm, String keyId) {
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

    protected AggregatedHttpResponse exchangeToken(String identityToken) {
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
  }

  @Nested
  class WildcardIssuer extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(
          ServerProperties.Property.ALLOWED_ISSUERS.getKey(), "http://localhost:*");
    }

    @Test
    public void testTokenExchange() throws IOException {
      AggregatedHttpResponse accepted =
          exchangeToken(
              createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId));
      assertThat(accepted.status()).isEqualTo(HttpStatus.OK);
      JsonNode body = MAPPER.readTree(accepted.contentUtf8());
      assertThat(body.has("access_token")).isTrue();

      AggregatedHttpResponse rejected =
          exchangeToken(
              createIdentityToken(
                  "https://evil.com", TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId));
      assertThat(rejected.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
      assertThat(rejected.contentUtf8()).contains("Invalid issuer");
    }
  }

  @Nested
  class AllowedCrossOriginJwks extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      testJwksUri = testJwksUri.replace("localhost", "127.0.0.1");
      serverProperties.setProperty(
          ServerProperties.Property.ALLOWED_JWKS_URIS.getKey(), testJwksUri);
    }

    @Test
    public void testTokenExchange() throws IOException {
      AggregatedHttpResponse response =
          exchangeToken(
              createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId));

      assertThat(response.status()).isEqualTo(HttpStatus.OK);
      assertThat(MAPPER.readTree(response.contentUtf8()).get("access_token").asText()).isNotEmpty();
    }
  }

  @Nested
  class RejectedCrossOriginJwks extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      testJwksUri = testJwksUri.replace("localhost", "127.0.0.1");
    }

    @Test
    public void testTokenExchange() throws IOException {
      AggregatedHttpResponse response =
          exchangeToken(
              createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId));

      assertThat(response.status()).isEqualTo(HttpStatus.CONFLICT);
      JsonNode body = MAPPER.readTree(response.contentUtf8());
      assertThat(body.get("error_code").asText()).isEqualTo("ABORTED");
      assertThat(body.get("message").asText()).isEqualTo("JWKS URI is not allowed");
    }
  }

  @Nested
  class WildcardAudiencePattern extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(
          ServerProperties.Property.AUDIENCES.getKey(),
          "unity-catalog-local,https://*.dev.example.com");
    }

    @Test
    public void testTokenExchange() throws IOException {
      AggregatedHttpResponse exactAudience =
          exchangeToken(
              createIdentityToken(
                  testIssuer, "unity-catalog-local", testIssuerAlgorithm, testIssuerKeyId));
      assertThat(exactAudience.status()).isEqualTo(HttpStatus.OK);
      JsonNode exactBody = MAPPER.readTree(exactAudience.contentUtf8());
      assertThat(exactBody.has("access_token")).isTrue();

      AggregatedHttpResponse wildcardAudience =
          exchangeToken(
              createIdentityToken(
                  testIssuer, "https://dev.dev.example.com", testIssuerAlgorithm, testIssuerKeyId));
      assertThat(wildcardAudience.status()).isEqualTo(HttpStatus.OK);

      AggregatedHttpResponse rejected =
          exchangeToken(
              createIdentityToken(
                  testIssuer, "dynamic-oauth-client-uuid", testIssuerAlgorithm, testIssuerKeyId));
      assertThat(rejected.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
      assertThat(rejected.contentUtf8()).contains("Invalid audience");
    }
  }

  @Nested
  class WildcardAudienceDisabled extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(ServerProperties.Property.AUDIENCES.getKey(), "*");
    }

    @Test
    public void testTokenExchange() throws IOException {
      AggregatedHttpResponse accepted =
          exchangeToken(
              createIdentityToken(
                  testIssuer, "dynamic-oauth-client-uuid", testIssuerAlgorithm, testIssuerKeyId));
      assertThat(accepted.status()).isEqualTo(HttpStatus.OK);
      JsonNode body = MAPPER.readTree(accepted.contentUtf8());
      assertThat(body.has("access_token")).isTrue();
      assertThat(body.get("access_token").asText()).isNotEmpty();

      AggregatedHttpResponse rejected =
          exchangeToken(
              createIdentityTokenForSubject(
                  testIssuer,
                  "any-audience",
                  "unknown-user@example.com",
                  testIssuerAlgorithm,
                  testIssuerKeyId));
      assertThat(rejected.status()).isEqualTo(HttpStatus.BAD_REQUEST);
      assertThat(rejected.contentUtf8()).contains("User not allowed");
    }
  }

  @Nested
  class MissingAllowedIssuers extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(ServerProperties.Property.ALLOWED_ISSUERS.getKey(), "");
    }

    @Test
    public void testTokenExchangeRejectsMissingAllowedIssuers() {
      String token =
          createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

      AggregatedHttpResponse response = exchangeToken(token);

      assertThat(response.status()).isEqualTo(HttpStatus.BAD_REQUEST);
      assertThat(response.contentUtf8())
          .contains(
              "No "
                  + WildcardAllowlist.ALLOWED_ISSUERS_NAME
                  + " configured. Set "
                  + ServerProperties.Property.ALLOWED_ISSUERS.getKey()
                  + " in server.properties");
    }
  }

  @Nested
  class MissingAudiences extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(ServerProperties.Property.AUDIENCES.getKey(), "");
    }

    @Test
    public void testTokenExchangeRejectsMissingAudiences() {
      String token =
          createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

      AggregatedHttpResponse response = exchangeToken(token);

      assertThat(response.status()).isEqualTo(HttpStatus.BAD_REQUEST);
      assertThat(response.contentUtf8())
          .contains(
              "No "
                  + WildcardAllowlist.AUDIENCES_NAME
                  + " configured. Set "
                  + ServerProperties.Property.AUDIENCES.getKey()
                  + " in server.properties");
    }
  }

  /**
   * {@code external} mode with the test IdP configured as {@code server.authentication.idp.test.*}
   * ({@link #subjectClaim()}). Tokens are signed with the test IdP's key, with claims as given.
   */
  abstract static class ExternalModeTestBase extends TokenExchangeTestBase {

    protected static final String CATALOGS = "/api/2.1/unity-catalog/catalogs";
    protected static final String SCIM_USERS = "/api/1.0/unity-control/scim2/Users";

    protected abstract String subjectClaim();

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty("server.authentication.type", "external");
      serverProperties.setProperty("server.authentication.idp.test.issuer", testIssuer);
      serverProperties.setProperty("server.authentication.idp.test.audience", TEST_AUDIENCE);
      serverProperties.setProperty("server.authentication.idp.test.subject-claim", subjectClaim());
    }

    /** A token from the test IdP with {@code claims}, its audience, and a 5 minute expiry. */
    protected String idpToken(Map<String, String> claims) {
      return idpToken(testIssuer, TEST_AUDIENCE, Instant.now().plusSeconds(300), claims);
    }

    protected String idpToken(
        String issuer, String audience, Instant expiresAt, Map<String, String> claims) {
      return idpToken(issuer, audience, expiresAt, Instant.now(), claims);
    }

    /** As above, issued at {@code issuedAt}, or with no {@code iat} when it is null. */
    protected String idpToken(
        String issuer,
        String audience,
        Instant expiresAt,
        Instant issuedAt,
        Map<String, String> claims) {
      var builder = JWT.create().withIssuer(issuer).withKeyId(testIssuerKeyId);
      if (issuedAt != null) {
        builder.withIssuedAt(issuedAt);
      }
      if (audience != null) {
        builder.withAudience(audience);
      }
      if (expiresAt != null) {
        builder.withExpiresAt(expiresAt);
      }
      claims.forEach(builder::withClaim);
      return builder.sign(testIssuerAlgorithm);
    }

    /**
     * A token for {@code person-1} from the test IdP's issuer and audience, valid for 5 minutes,
     * with {@code claims} applied last.
     */
    protected String person1Token(UnaryOperator<JWTCreator.Builder> claims) {
      return claims
          .apply(
              JWT.create()
                  .withIssuer(testIssuer)
                  .withKeyId(testIssuerKeyId)
                  .withAudience(TEST_AUDIENCE)
                  .withExpiresAt(Instant.now().plusSeconds(300))
                  .withClaim("sub", "person-1"))
          .sign(testIssuerAlgorithm);
    }

    protected AggregatedHttpResponse call(
        HttpMethod method, String path, String token, String body) {
      RequestHeaders headers =
          RequestHeaders.builder(method, path)
              .contentType(MediaType.JSON)
              .add(HttpHeaderNames.AUTHORIZATION, "Bearer " + token)
              .build();
      return client
          .execute(headers, body == null ? HttpData.empty() : HttpData.ofUtf8(body))
          .aggregate()
          .join();
    }

    protected AggregatedHttpResponse listCatalogs(String token) {
      return call(HttpMethod.GET, CATALOGS, token, null);
    }

    /** Asserts the refusal's status, error code and message. */
    protected static void assertRefused(
        AggregatedHttpResponse response, HttpStatus status, String message) throws IOException {
      assertThat(response.status()).isEqualTo(status);
      JsonNode error = MAPPER.readTree(response.contentUtf8());
      assertThat(error.get("message").asText()).isEqualTo(message);
    }

    /** The admin sets {@code active} with Okta's PATCH request. */
    protected void setActive(String email, boolean active) {
      String admin = securityContext.createServiceToken();
      String id = userId(email);
      String patch =
          """
          {"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
          "Operations":[{"op":"replace","value":{"active":%s}}]}\
          """
              .formatted(active);
      assertThat(call(HttpMethod.PATCH, SCIM_USERS + "/" + id, admin, patch).status())
          .isEqualTo(HttpStatus.OK);
    }

    @SneakyThrows
    protected String userId(String email) {
      AggregatedHttpResponse users =
          call(HttpMethod.GET, SCIM_USERS, securityContext.createServiceToken(), null);
      for (JsonNode user : MAPPER.readTree(users.contentUtf8()).get("Resources")) {
        if (user.get("emails").get(0).get("value").asText().equals(email)) {
          return user.get("id").asText();
        }
      }
      throw new IllegalStateException("No user " + email);
    }
  }

  /** IdP access tokens sent directly, with {@code sub} naming the principal (SUB). */
  @Nested
  class ExternalModeSub extends ExternalModeTestBase {

    @Override
    protected String subjectClaim() {
      return "SUB";
    }

    @Test
    public void testIdpTokensAcceptedDirectly() throws IOException {
      // The first call links the row with the token's email; later calls match the sub only.
      String first = idpToken(Map.of("sub", "person-1", "email", ENABLED_USER_EMAIL));
      assertThat(listCatalogs(first).status()).isEqualTo(HttpStatus.OK);
      assertThat(listCatalogs(idpToken(Map.of("sub", "person-1"))).status())
          .isEqualTo(HttpStatus.OK);
      assertThat(
              listCatalogs(idpToken(Map.of("sub", "person-1", "email", "renamed@example.com")))
                  .status())
          .isEqualTo(HttpStatus.OK);
      // The principal is the linked user, not the token's sub.
      AggregatedHttpResponse me =
          call(HttpMethod.GET, "/api/1.0/unity-control/scim2/Me", first, null);
      assertThat(me.status()).isEqualTo(HttpStatus.OK);
      assertThat(MAPPER.readTree(me.contentUtf8()).get("emails").get(0).get("value").asText())
          .isEqualTo(ENABLED_USER_EMAIL);
      // A token's own "type" claim doesn't make it a SCIM token: SCIM needs metastore OWNER.
      assertRefused(
          call(
              HttpMethod.GET,
              SCIM_USERS,
              idpToken(Map.of("sub", "person-1", "type", "SCIM")),
              null),
          HttpStatus.FORBIDDEN,
          "Access denied.");

      Map<String, String> notAllowed =
          Map.of(
              // The row is linked to person-1 already.
              ENABLED_USER_EMAIL,
              "person-2",
              // The bootstrap admin is never linked.
              "admin",
              "person-admin",
              // TODO(#2203): no row yet; then the first call creates one.
              "nobody@example.com",
              "person-nobody");
      for (Map.Entry<String, String> entry : notAllowed.entrySet()) {
        assertRefused(
            listCatalogs(idpToken(Map.of("sub", entry.getValue(), "email", entry.getKey()))),
            HttpStatus.FORBIDDEN,
            "User not allowed: " + entry.getKey());
      }

      Map<String, String> unauthenticated =
          Map.of(
              idpToken(Map.of("sub", "person-3")),
              "Access token has no email claim to link a user; add one in the identity provider. A"
                  + " token issued before the user's last reactivation is never linked.",
              idpToken(Map.of("email", ENABLED_USER_EMAIL)),
              "Access token has no sub claim.",
              idpToken(
                  testIssuer,
                  "other-audience",
                  Instant.now().plusSeconds(300),
                  Map.of("sub", "person-1")),
              "Invalid audience",
              idpToken(
                  testIssuer,
                  TEST_AUDIENCE,
                  Instant.now().minusSeconds(60),
                  Map.of("sub", "person-1")),
              "Invalid access token.",
              idpToken(testIssuer, TEST_AUDIENCE, null, Map.of("sub", "person-1")),
              "Access token has no expiry.",
              idpToken(testIssuer, null, Instant.now().plusSeconds(300), Map.of("sub", "person-1")),
              "Invalid audience",
              withoutAlgorithm(idpToken(Map.of("sub", "person-1"))),
              "Missing signing algorithm");
      for (Map.Entry<String, String> entry : unauthenticated.entrySet()) {
        assertRefused(listCatalogs(entry.getKey()), HttpStatus.UNAUTHORIZED, entry.getValue());
      }
      // An issuer that no block names, or none at all.
      for (String issuer : Arrays.asList("https://unknown.example.com", null)) {
        assertRefused(
            listCatalogs(
                idpToken(
                    issuer,
                    TEST_AUDIENCE,
                    Instant.now().plusSeconds(300),
                    Map.of("sub", "person-1"))),
            HttpStatus.FORBIDDEN,
            "Invalid access token.");
      }

      // An IdP clock up to 60 seconds ahead of the server's is accepted on iat and nbf; exp has
      // no leeway.
      Instant now = Instant.now();
      for (UnaryOperator<JWTCreator.Builder> claims :
          List.<UnaryOperator<JWTCreator.Builder>>of(
              b -> b.withIssuedAt(now.plusSeconds(30)),
              b -> b.withNotBefore(now.plusSeconds(55)))) {
        assertThat(listCatalogs(person1Token(claims)).status()).isEqualTo(HttpStatus.OK);
      }
      for (UnaryOperator<JWTCreator.Builder> claims :
          List.<UnaryOperator<JWTCreator.Builder>>of(
              b -> b.withIssuedAt(now.plusSeconds(75)),
              b -> b.withNotBefore(now.plusSeconds(75)),
              b -> b.withExpiresAt(now.minusSeconds(5)))) {
        assertRefused(
            listCatalogs(person1Token(claims)), HttpStatus.UNAUTHORIZED, "Invalid access token.");
      }

      // A disabled user's token is refused on the next call.
      setActive(ENABLED_USER_EMAIL, false);
      assertRefused(
          listCatalogs(first), HttpStatus.FORBIDDEN, "User not allowed: " + ENABLED_USER_EMAIL);

      // The token exchange is off.
      assertRefused(
          exchangeToken(idpToken(Map.of("sub", "person-1", "email", ENABLED_USER_EMAIL))),
          HttpStatus.NOT_IMPLEMENTED,
          "Token exchange is disabled in external mode");
    }

    @Test
    public void testIdpTokenRefusedOnceItsUserIsAnIdpPrincipal() throws IOException {
      String token = idpToken(Map.of("sub", "person-1", "email", ENABLED_USER_EMAIL));
      assertThat(listCatalogs(token).status()).isEqualTo(HttpStatus.OK);
      // The admin then makes the linked user an IdP principal; only its SCIM token works now.
      String scimToken =
          AdminTokenCommand.createScimToken(
              new String[] {
                "--scim-principal", ENABLED_USER_EMAIL, "--idp", "okta", "--lifetime", "PT1H"
              },
              securityContext.getAlgorithm(),
              new UserRepository(null, hibernateConfigurator.getSessionFactory()));
      for (String path : new String[] {CATALOGS, "/api/1.0/unity-control/scim2/Me"}) {
        assertRefused(
            call(HttpMethod.GET, path, token, null),
            HttpStatus.FORBIDDEN,
            "User not allowed: " + ENABLED_USER_EMAIL);
      }
      assertThat(call(HttpMethod.GET, SCIM_USERS, scimToken, null).status())
          .isEqualTo(HttpStatus.OK);
    }
  }

  /** {@code internal} mode ignores an IdP block: the IdP's token is not accepted directly. */
  @Nested
  class InternalModeWithIdentityProviderBlock extends ExternalModeTestBase {

    @Override
    protected String subjectClaim() {
      return "SUB";
    }

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty("server.authentication.type", "internal");
    }

    @Test
    public void testIdpTokenRefused() throws IOException {
      assertRefused(
          listCatalogs(idpToken(Map.of("sub", "person-1", "email", ENABLED_USER_EMAIL))),
          HttpStatus.FORBIDDEN,
          "Invalid access token.");
    }
  }

  /** {@code token} with its header's {@code alg} removed; the signature is left as it was. */
  @SneakyThrows
  private static String withoutAlgorithm(String token) {
    String[] parts = token.split("\\.");
    ObjectNode header = (ObjectNode) MAPPER.readTree(Base64.getUrlDecoder().decode(parts[0]));
    header.remove("alg");
    return Base64.getUrlEncoder().withoutPadding().encodeToString(MAPPER.writeValueAsBytes(header))
        + "."
        + parts[1]
        + "."
        + parts[2];
  }

  /** Okta: {@code uid} names a person, else {@code cid} a machine; {@code sub} is never read. */
  @Nested
  class ExternalModeOkta extends ExternalModeTestBase {

    private static final String MACHINE_EMAIL = "etl@example.com";

    @Override
    protected String subjectClaim() {
      return "OKTA";
    }

    @Test
    public void testOktaClaimsAndReactivation() throws IOException {
      createEnabledUser(MACHINE_EMAIL);
      // A person's token carries uid, cid and an editable sub that defaults to the login. Issued
      // a minute ago, so it is older than the reactivation below.
      String person =
          idpToken(
              testIssuer,
              TEST_AUDIENCE,
              Instant.now().plusSeconds(300),
              Instant.now().minusSeconds(60),
              Map.of(
                  "uid",
                  "00u-first",
                  "cid",
                  "0oa-cli",
                  "sub",
                  ENABLED_USER_EMAIL,
                  "email",
                  ENABLED_USER_EMAIL));
      assertThat(listCatalogs(person).status()).isEqualTo(HttpStatus.OK);
      assertThat(listCatalogs(idpToken(Map.of("uid", "00u-first", "sub", "renamed"))).status())
          .isEqualTo(HttpStatus.OK);
      // The same sub and email with another uid is another person.
      assertRefused(
          listCatalogs(
              idpToken(
                  Map.of(
                      "uid",
                      "00u-other",
                      "cid",
                      "0oa-cli",
                      "sub",
                      ENABLED_USER_EMAIL,
                      "email",
                      ENABLED_USER_EMAIL))),
          HttpStatus.FORBIDDEN,
          "User not allowed: " + ENABLED_USER_EMAIL);

      // A machine's token has cid and no uid; it links by its email claim, then by cid.
      assertThat(listCatalogs(idpToken(Map.of("cid", "0oa-etl", "email", MACHINE_EMAIL))).status())
          .isEqualTo(HttpStatus.OK);
      assertThat(listCatalogs(idpToken(Map.of("cid", "0oa-etl"))).status())
          .isEqualTo(HttpStatus.OK);
      assertRefused(
          listCatalogs(idpToken(Map.of("sub", "0oa-etl", "email", MACHINE_EMAIL))),
          HttpStatus.UNAUTHORIZED,
          "Access token has no uid or cid claim.");

      // Okta reactivates the row for a rehire with a new uid: the link is cleared, so the new
      // uid links by email and the old one no longer matches. A token issued before the
      // reactivation, or with no iat, can't link the row, even when it calls first.
      setActive(ENABLED_USER_EMAIL, false);
      setActive(ENABLED_USER_EMAIL, true);
      assertRefused(
          listCatalogs(person), HttpStatus.FORBIDDEN, "User not allowed: " + ENABLED_USER_EMAIL);
      assertRefused(
          listCatalogs(
              idpToken(
                  testIssuer,
                  TEST_AUDIENCE,
                  Instant.now().plusSeconds(300),
                  null,
                  Map.of("uid", "00u-first", "email", ENABLED_USER_EMAIL))),
          HttpStatus.FORBIDDEN,
          "User not allowed: " + ENABLED_USER_EMAIL);
      assertThat(
              listCatalogs(idpToken(Map.of("uid", "00u-rehire", "email", ENABLED_USER_EMAIL)))
                  .status())
          .isEqualTo(HttpStatus.OK);
      assertRefused(
          listCatalogs(person), HttpStatus.FORBIDDEN, "User not allowed: " + ENABLED_USER_EMAIL);

      // Okta reactivates the same person (same uid): the new token links, and a token issued
      // before the reactivation, or without iat, stays refused on every later call.
      Map<String, String> rehired = Map.of("uid", "00u-rehire", "email", ENABLED_USER_EMAIL);
      String beforeReactivation =
          idpToken(
              testIssuer,
              TEST_AUDIENCE,
              Instant.now().plusSeconds(300),
              Instant.now().minusSeconds(60),
              rehired);
      String withoutIssuedAt =
          idpToken(testIssuer, TEST_AUDIENCE, Instant.now().plusSeconds(300), null, rehired);
      setActive(ENABLED_USER_EMAIL, false);
      setActive(ENABLED_USER_EMAIL, true);
      String rejoined = idpToken(rehired);
      assertThat(listCatalogs(rejoined).status()).isEqualTo(HttpStatus.OK);
      for (String refused : new String[] {beforeReactivation, withoutIssuedAt}) {
        assertRefused(
            listCatalogs(refused), HttpStatus.FORBIDDEN, "User not allowed: " + ENABLED_USER_EMAIL);
      }
      // Without an email claim it can't be linked either; the message says why.
      assertRefused(
          listCatalogs(
              idpToken(
                  testIssuer,
                  TEST_AUDIENCE,
                  Instant.now().plusSeconds(300),
                  Instant.now().minusSeconds(60),
                  Map.of("uid", "00u-rehire"))),
          HttpStatus.UNAUTHORIZED,
          "Access token has no email claim to link a user; add one in the identity provider. A"
              + " token issued before the user's last reactivation is never linked.");
      assertThat(listCatalogs(rejoined).status()).isEqualTo(HttpStatus.OK);
    }
  }

  /**
   * Journey against a real Keycloak: a person's token and a service account's token are accepted
   * directly; a token with another audience, an expired one and one from an unconfigured issuer are
   * refused. Of this server's tokens only the bootstrap admin's SERVICE token and an IdP's SCIM
   * token still work.
   */
  @Nested
  class ExternalModeKeycloak extends ExternalModeTestBase {

    private final KeycloakIdentityProvider keycloak = KeycloakIdentityProvider.get();

    @Override
    protected String subjectClaim() {
      return "SUB";
    }

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(
          "server.authentication.idp.keycloak.issuer",
          keycloak.issuer(KeycloakIdentityProvider.REALM));
      serverProperties.setProperty(
          "server.authentication.idp.keycloak.audience", KeycloakIdentityProvider.AUDIENCE);
    }

    @Test
    public void testKeycloakTokensJourney() throws Exception {
      // The admin creates the person and the machine before their first calls.
      createEnabledUser(KeycloakIdentityProvider.PERSON_EMAIL);
      createEnabledUser(KeycloakIdentityProvider.MACHINE_EMAIL);
      String personToken = keycloak.personToken();
      assertThat(listCatalogs(personToken).status()).isEqualTo(HttpStatus.OK);
      String machineToken =
          keycloak.clientCredentialsToken(
              KeycloakIdentityProvider.REALM, KeycloakIdentityProvider.MACHINE_CLIENT);
      assertThat(listCatalogs(machineToken).status()).isEqualTo(HttpStatus.OK);
      assertThat(JWT.decode(machineToken).getClaim("email").asString())
          .isEqualTo(KeycloakIdentityProvider.MACHINE_EMAIL);

      String shortLived =
          keycloak.clientCredentialsToken(
              KeycloakIdentityProvider.REALM, KeycloakIdentityProvider.SHORT_LIVED_CLIENT);
      String otherAudience =
          keycloak.clientCredentialsToken(
              KeycloakIdentityProvider.REALM, KeycloakIdentityProvider.OTHER_AUDIENCE_CLIENT);
      String otherIssuer =
          keycloak.clientCredentialsToken(
              KeycloakIdentityProvider.OTHER_REALM, KeycloakIdentityProvider.MACHINE_CLIENT);
      assertRefused(listCatalogs(otherAudience), HttpStatus.UNAUTHORIZED, "Invalid audience");
      assertRefused(listCatalogs(otherIssuer), HttpStatus.FORBIDDEN, "Invalid access token.");
      // The short-lived token expires one second after it is issued.
      Thread.sleep(
          Math.max(
              0,
              JWT.decode(shortLived).getExpiresAtAsInstant().toEpochMilli()
                  - System.currentTimeMillis()
                  + 1100));
      assertRefused(listCatalogs(shortLived), HttpStatus.UNAUTHORIZED, "Invalid access token.");

      // Of this server's own tokens: an ACCESS token is refused; the bootstrap admin's SERVICE
      // token and an IdP's SCIM token still work.
      assertRefused(
          listCatalogs(
              securityContext.createAccessToken(
                  KeycloakIdentityProvider.PERSON_EMAIL, Duration.ofMinutes(5))),
          HttpStatus.UNAUTHORIZED,
          "Access tokens from this server are not accepted in external authentication mode;"
              + " send the identity provider's access token.");
      assertThat(listCatalogs(securityContext.createServiceToken()).status())
          .isEqualTo(HttpStatus.OK);
      createEnabledUser("keycloak-scim");
      String scimToken =
          AdminTokenCommand.createScimToken(
              new String[] {
                "--scim-principal", "keycloak-scim", "--idp", "keycloak", "--lifetime", "PT1H"
              },
              securityContext.getAlgorithm(),
              new UserRepository(null, hibernateConfigurator.getSessionFactory()));
      assertThat(call(HttpMethod.GET, SCIM_USERS, scimToken, null).status())
          .isEqualTo(HttpStatus.OK);
    }
  }
}
