package io.unitycatalog.cli.auth;

import static io.unitycatalog.cli.TestUtils.executeCliCommand;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.cli.AuthCli;
import io.unitycatalog.cli.utils.Oauth2CliExchange;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class CliAuthCrudTest extends BaseAuthCRUDTest {

  private static final String ENABLED_USER_EMAIL = "test-user@example.com";

  /**
   * Creates a signed identity token for token exchange tests.
   *
   * @param subject the principal asserted by the token (used as the {@code sub} claim)
   * @param issuer the token issuer
   * @param audience the token audience (may be null)
   * @param algorithm the signing algorithm to use
   * @param keyId the key ID to include in the JWT header
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

  @Test
  public void testAuthLoginExchangeWithCorrectIssuerAndAudience() throws IOException {
    System.out.println("Testing login exchange with correct issuer and audience..");

    // The exchanged principal must be an enabled user, so create one with the bootstrap admin
    // token before exchanging as that user.
    String adminToken = securityContext.createServiceToken();
    serverConfig.setAuthToken(adminToken);
    executeCliCommand(
        serverConfig,
        List.of("user", "create", "--email", ENABLED_USER_EMAIL, "--name", "Test User"));
    serverConfig.setAuthToken("");

    String token =
        createIdentityToken(
            ENABLED_USER_EMAIL, testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

    List<String> argsList = List.of("auth", "login", "--identity_token", token);

    JsonNode authExchangeInfo = executeCliCommand(serverConfig, argsList);
    assertThat(authExchangeInfo.get("access_token")).isNotNull();
  }

  @Test
  public void testAuthLoginExchangeWithCorrectIssuerAndWrongAudience() {
    System.out.println("Testing login exchange with correct issuer but wrong audience..");

    String token =
        createIdentityToken(
            ENABLED_USER_EMAIL, testIssuer, "wrong-audience", testIssuerAlgorithm, testIssuerKeyId);

    List<String> argsList = List.of("auth", "login", "--identity_token", token);
    assertThatThrownBy(() -> executeCliCommand(serverConfig, argsList))
        .isInstanceOf(RuntimeException.class);
  }

  private static final String IDP_ACCESS_TOKEN = "idp-access-token";

  /** The body UC sends for an error (GlobalExceptionHandler). */
  private static String ucError(String errorCode) {
    return "{\"error_code\":\"" + errorCode + "\",\"message\":\"Token exchange is disabled\"}";
  }

  /**
   * Starts a server whose {@code /auth/tokens} answers with the given status and body, and passes
   * the request body it received to {@code received}.
   */
  private HttpServer startTokenEndpoint(int status, String body, AtomicReference<String> received)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/api/1.0/unity-control/auth/tokens",
        exchange -> {
          received.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(status, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.close();
        });
    server.start();
    return server;
  }

  private static ApiClient clientFor(int port) {
    ApiClient apiClient = new ApiClient();
    apiClient.setScheme("http");
    apiClient.setHost("localhost");
    apiClient.setPort(port);
    apiClient.setBasePath("/api/1.0/unity-control");
    return apiClient;
  }

  private static ApiClient clientFor(HttpServer server) {
    return clientFor(server.getAddress().getPort());
  }

  private ApiClient clientForTestServer() {
    return clientFor(Integer.parseInt(serverConfig.getServerUrl().replaceAll(".*:", "")));
  }

  // Only a 501 whose UC error body says UNIMPLEMENTED switches to the access token. Anything else
  // (a proxy's 501, another error, another status) is an error.
  @Test
  public void testAuthLoginKeepsTheIdpAccessTokenOnlyForAnUnimplementedError() throws Exception {
    record Reply(int status, String body) {}
    for (Reply reply :
        List.of(
            new Reply(501, "<html>501 Not Implemented</html>"),
            new Reply(501, ucError("INTERNAL")),
            new Reply(500, ucError("UNIMPLEMENTED")))) {
      HttpServer server = startTokenEndpoint(reply.status(), reply.body(), new AtomicReference<>());
      try {
        assertThatThrownBy(
                () ->
                    AuthCli.exchangeForAccessToken(
                        clientFor(server), "the-id-token", IDP_ACCESS_TOKEN))
            .as("%s", reply)
            .isInstanceOf(ApiException.class)
            .hasMessageContaining("Error authenticating - " + reply.body());
      } finally {
        server.stop(0);
      }
    }
  }

  // Internal mode: the exchange works, and the CLI returns the UC token, never the access token
  // the identity provider issued.
  @Test
  public void testAuthLoginInInternalModeReturnsTheExchangedToken() throws Exception {
    serverConfig.setAuthToken(securityContext.createServiceToken());
    executeCliCommand(
        serverConfig,
        List.of("user", "create", "--email", ENABLED_USER_EMAIL, "--name", "Test User"));
    serverConfig.setAuthToken("");
    String identityToken =
        createIdentityToken(
            ENABLED_USER_EMAIL, testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);
    ApiClient apiClient = clientForTestServer();

    String accessToken = AuthCli.exchangeForAccessToken(apiClient, identityToken, IDP_ACCESS_TOKEN);

    assertThat(accessToken).isNotEqualTo(IDP_ACCESS_TOKEN);
    assertThat(JWT.decode(accessToken).getIssuer()).isEqualTo(securityContext.getLocalIssuer());
    // A failed exchange (wrong audience) is still an error, not a fallback to the access token.
    String wrongAudience =
        createIdentityToken(
            ENABLED_USER_EMAIL, testIssuer, "wrong-audience", testIssuerAlgorithm, testIssuerKeyId);
    assertThatThrownBy(
            () -> AuthCli.exchangeForAccessToken(apiClient, wrongAudience, IDP_ACCESS_TOKEN))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Error authenticating - ");
  }

  // Set by a test that needs an external mode server. JUnit makes a new test instance per method,
  // so the other tests stay in internal mode.
  private boolean isExternalMode;

  /**
   * In external mode the test IdP is configured as {@code server.authentication.idp.test.*}, as in
   * AuthServiceAllowlistTest.ExternalModeSub.
   */
  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    if (isExternalMode) {
      serverProperties.setProperty("server.authentication.type", "external");
      serverProperties.setProperty("server.authentication.idp.test.issuer", testIssuer);
      serverProperties.setProperty("server.authentication.idp.test.audience", TEST_AUDIENCE);
      serverProperties.setProperty("server.authentication.idp.test.subject-claim", "SUB");
    }
  }

  // The real server answers the exchange with 501 UNIMPLEMENTED, so `auth login` keeps the IdP
  // access token, and that token works on a catalog list. A pasted identity token has no access
  // token to keep.
  @Test
  public void testAuthLoginKeepsTheIdpAccessTokenInExternalMode() throws Exception {
    isExternalMode = true;
    tearDown();
    setUp();
    createEnabledUser(ENABLED_USER_EMAIL);
    String identityToken =
        createIdentityToken(
            ENABLED_USER_EMAIL, testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);
    String idpAccessToken =
        JWT.create()
            .withIssuer(testIssuer)
            .withKeyId(testIssuerKeyId)
            .withAudience(TEST_AUDIENCE)
            .withIssuedAt(Instant.now())
            .withExpiresAt(Instant.now().plusSeconds(300))
            .withClaim("sub", "person-1")
            .withClaim("email", ENABLED_USER_EMAIL)
            .sign(testIssuerAlgorithm);
    ApiClient apiClient = clientForTestServer();

    assertThat(AuthCli.exchangeForAccessToken(apiClient, identityToken, idpAccessToken))
        .isEqualTo(idpAccessToken);

    serverConfig.setAuthToken(idpAccessToken);
    assertThat(executeCliCommand(serverConfig, List.of("catalog", "list"))).isNotNull();

    serverConfig.setAuthToken("");
    assertThatThrownBy(
            () ->
                executeCliCommand(
                    serverConfig, List.of("auth", "login", "--identity_token", identityToken)))
        .isInstanceOf(RuntimeException.class)
        .rootCause()
        .hasMessageStartingWith(
            "Error authenticating - token exchange is disabled on this server; use the identity"
                + " provider's access token as --auth_token - ")
        .hasMessageContaining("\"error_code\":\"UNIMPLEMENTED\"")
        .hasMessageContaining("Token exchange is disabled in external mode");
  }

  // bin/uc auth login without --identity_token reads the client secret for its OAuth flow.
  @Test
  public void testAuthLoginReadsTheClientSecretFromTheEnvironmentBeforeTheFile() {
    Properties file = new Properties();
    file.setProperty("server.client-secret", "file-secret");

    // A line break at the end, as in a Secret made from a file, is dropped.
    assertThat(
            Oauth2CliExchange.clientSecret(
                Map.of("UC_SERVER_CLIENT_SECRET", "env-secret\r\n"), file))
        .isEqualTo("env-secret");
    // An absent, empty or line-break-only variable falls back to the file.
    for (Map<String, String> env :
        List.of(
            Map.<String, String>of(),
            Map.of("UC_SERVER_CLIENT_SECRET", ""),
            Map.of("UC_SERVER_CLIENT_SECRET", "\n"))) {
      assertThat(Oauth2CliExchange.clientSecret(env, file)).isEqualTo("file-secret");
    }
  }

  // bin/uc auth login keeps the identity provider's access token from its token response.
  @Test
  public void testAuthLoginReadsTheTokensFromTheIdentityProviderResponse() throws IOException {
    Oauth2CliExchange.IdpTokens both =
        Oauth2CliExchange.idpTokens(
            "{\"access_token\":\"idp-access-token\",\"id_token\":\"idp-id-token\","
                + "\"token_type\":\"Bearer\",\"expires_in\":3600}");
    assertThat(both.accessToken()).isEqualTo("idp-access-token");
    assertThat(both.idToken()).isEqualTo("idp-id-token");

    Oauth2CliExchange.IdpTokens noAccessToken =
        Oauth2CliExchange.idpTokens("{\"id_token\":\"idp-id-token\"}");
    assertThat(noAccessToken.accessToken()).isNull();
    assertThat(noAccessToken.idToken()).isEqualTo("idp-id-token");
  }

  @Test
  public void testAuthLoginExchangeWithWrongIssuerAndCorrectAudience()
      throws NoSuchAlgorithmException {
    System.out.println("Testing login exchange with wrong issuer and correct audience..");

    // Generate a separate RSA keypair to simulate a foreign identity provider.
    // The token is signed with keys the server doesn't know about, and the issuer
    // is not in the server's allowlist.
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

    List<String> argsList = List.of("auth", "login", "--identity_token", token);
    assertThatThrownBy(() -> executeCliCommand(serverConfig, argsList))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void testAuthAccess()
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    System.out.println("Testing access..");

    List<String> argsList = List.of("catalog", "list");

    // Test with no Authentication on authenticated end point
    assertThatThrownBy(
            () -> {
              executeCliCommand(serverConfig, argsList);
            })
        .isInstanceOf(RuntimeException.class);

    // Test with Authentication on authenticated end point
    String token = securityContext.createServiceToken();

    serverConfig.setAuthToken(token);

    JsonNode responseJsonInfo = executeCliCommand(serverConfig, argsList);
    assertThat(responseJsonInfo).isNotNull();

    // Test with authentication on authenticated end point with missing user
    String missingUserJwt =
        JWT.create()
            .withSubject(securityContext.getServiceName())
            .withIssuer(securityContext.getLocalIssuer())
            .withIssuedAt(new Date())
            .withExpiresAt(Date.from(Instant.now().plus(Duration.ofHours(1))))
            .withJWTId(UUID.randomUUID().toString())
            .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.ACCESS.name())
            .withClaim(JwtClaim.SUBJECT.key(), "missing@localhost")
            .sign(securityConfiguration.algorithmRSA());

    serverConfig.setAuthToken(missingUserJwt);

    assertThatThrownBy(
            () -> {
              executeCliCommand(serverConfig, argsList);
            })
        .isInstanceOf(RuntimeException.class);

    serverConfig.setAuthToken(token);

    // Test adding a user
    List<String> argsAddUser =
        List.of("user", "create", "--email", "test@localhost", "--name", "Test User");
    responseJsonInfo = executeCliCommand(serverConfig, argsAddUser);
    assertThat(responseJsonInfo).isNotNull();

    // Test with authentication on authenticated end point with added user
    String testUserJwt =
        JWT.create()
            .withSubject(securityContext.getServiceName())
            .withIssuer(securityContext.getLocalIssuer())
            .withIssuedAt(new Date())
            .withExpiresAt(Date.from(Instant.now().plus(Duration.ofHours(1))))
            .withJWTId(UUID.randomUUID().toString())
            .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.ACCESS.name())
            .withClaim(JwtClaim.SUBJECT.key(), "test@localhost")
            .sign(securityConfiguration.algorithmRSA());

    serverConfig.setAuthToken(testUserJwt);
    responseJsonInfo = executeCliCommand(serverConfig, argsList);
    assertThat(responseJsonInfo).isNotNull();
  }

  @Test
  public void testUserCrud() throws IOException {
    // Test with Authentication on authenticated end point
    String token = securityContext.createServiceToken();

    serverConfig.setAuthToken(token);
    // Test creating a user
    List<String> argsAddUser =
        List.of("user", "create", "--email", "user@localhost", "--name", "Test User");
    JsonNode responseJsonInfo = executeCliCommand(serverConfig, argsAddUser);
    assertThat(responseJsonInfo).isNotNull();
    assertThat(responseJsonInfo.get("name").asText()).isEqualTo("Test User");
    assertThat(responseJsonInfo.get("email").asText()).isEqualTo("user@localhost");

    String id = responseJsonInfo.get("id").asText();

    // Test creating a user that already exists.
    assertThatThrownBy(
            () -> {
              executeCliCommand(serverConfig, argsAddUser);
            })
        .isInstanceOf(RuntimeException.class);

    // Test updating a user
    List<String> argsUpdateUser =
        List.of(
            "user", "update", "--id", id, "--name", "Test User Updated", "--external_id", "123");
    responseJsonInfo = executeCliCommand(serverConfig, argsUpdateUser);
    assertThat(responseJsonInfo).isNotNull();
    assertThat(responseJsonInfo.get("name").asText()).isEqualTo("Test User Updated");
    assertThat(responseJsonInfo.get("external_id").asText()).isEqualTo("123");

    // Test getting a user
    List<String> argsGetUser = List.of("user", "get", "--id", id);
    responseJsonInfo = executeCliCommand(serverConfig, argsGetUser);
    assertThat(responseJsonInfo).isNotNull();
    assertThat(responseJsonInfo.get("name").asText()).isEqualTo("Test User Updated");

    // Test listing users
    List<String> argsListUsers = List.of("user", "list");
    responseJsonInfo = executeCliCommand(serverConfig, argsListUsers);
    assertThat(responseJsonInfo).isNotNull();
    assertThat(responseJsonInfo).hasSize(2);

    // Test deleting a user
    List<String> argsDeleteUser = List.of("user", "delete", "--id", id);
    responseJsonInfo = executeCliCommand(serverConfig, argsDeleteUser);
    assertThat(responseJsonInfo).isNotNull();

    serverConfig.setAuthToken("");
  }
}
