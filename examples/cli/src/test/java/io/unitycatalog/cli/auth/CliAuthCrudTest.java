package io.unitycatalog.cli.auth;

import static io.unitycatalog.cli.TestUtils.executeCliCommand;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.JsonNode;
import io.unitycatalog.cli.utils.Oauth2CliExchange;
import io.unitycatalog.control.ApiException;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.StreamSupport;
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
    String adminToken = Files.readString(configurationFolder.resolve("token.txt"));
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
    Path path = configurationFolder.resolve("token.txt");
    String token = Files.readString(path);

    serverConfig.setAuthToken(token);

    JsonNode responseJsonInfo = executeCliCommand(serverConfig, argsList);
    assertThat(responseJsonInfo).isNotNull();

    // Test with authentication on authenticated end point with missing user
    String missingUserJwt =
        JWT.create()
            .withSubject(securityContext.getServiceName())
            .withIssuer(securityContext.getLocalIssuer())
            .withIssuedAt(new Date())
            .withKeyId(securityConfiguration.getKeyId())
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
            .withKeyId(securityConfiguration.getKeyId())
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
    Path path = configurationFolder.resolve("token.txt");
    String token = Files.readString(path);

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

  /** Runs a CLI command that the server refuses, returning the control API's error. */
  private ApiException refusedCommand(List<String> args) {
    Throwable thrown = catchThrowable(() -> executeCliCommand(serverConfig, args));
    assertThat(thrown).isInstanceOf(RuntimeException.class).hasCauseInstanceOf(ApiException.class);
    return (ApiException) thrown.getCause();
  }

  private String createUserId(String email) {
    return executeCliCommand(
            serverConfig, List.of("user", "create", "--email", email, "--name", email))
        .get("id")
        .asText();
  }

  private static List<String> memberIds(JsonNode group) {
    return StreamSupport.stream(group.get("members").spliterator(), false)
        .map(JsonNode::asText)
        .toList();
  }

  @Test
  public void testGroupCrud() throws IOException {
    serverConfig.setAuthToken(Files.readString(Path.of("etc", "conf", "token.txt")));
    String ada = createUserId("ada@localhost");
    String bob = createUserId("bob@localhost");

    // Create, with an external id, and add both users.
    JsonNode group =
        executeCliCommand(
            serverConfig,
            List.of("group", "create", "--name", "data-eng", "--external_id", "okta-1"));
    String id = group.get("id").asText();
    assertThat(group.get("name").asText()).isEqualTo("data-eng");
    assertThat(group.get("external_id").asText()).isEqualTo("okta-1");
    assertThat(memberIds(group)).isEmpty();
    for (String user : List.of(ada, bob)) {
      executeCliCommand(serverConfig, List.of("group", "add-member", "--id", id, "--member", user));
    }
    group = executeCliCommand(serverConfig, List.of("group", "get", "--id", id));
    assertThat(memberIds(group)).containsExactlyInAnyOrder(ada, bob);

    // Both are repeatable: adding a member twice and removing a non-member change nothing.
    group =
        executeCliCommand(
            serverConfig, List.of("group", "add-member", "--id", id, "--member", bob));
    assertThat(memberIds(group)).containsExactlyInAnyOrder(ada, bob);
    for (int attempt = 0; attempt < 2; attempt++) {
      group =
          executeCliCommand(
              serverConfig, List.of("group", "remove-member", "--id", id, "--member", ada));
      assertThat(memberIds(group)).containsExactly(bob);
    }

    // A second group, created with several members: ids are split on commas and trimmed, and an
    // empty entry is skipped. Then a duplicate name.
    for (String members : List.of(ada + "," + bob, " " + ada + " , " + bob + ",,")) {
      group =
          executeCliCommand(
              serverConfig, List.of("group", "create", "--name", "empty", "--members", members));
      assertThat(memberIds(group)).as(members).containsExactlyInAnyOrder(ada, bob);
      executeCliCommand(serverConfig, List.of("group", "delete", "--id", group.get("id").asText()));
    }
    group =
        executeCliCommand(
            serverConfig, List.of("group", "create", "--name", "empty", "--members", ada));
    assertThat(memberIds(group)).containsExactly(ada);
    ApiException duplicate =
        refusedCommand(List.of("group", "create", "--name", "data-eng", "--members", ada));
    assertThat(duplicate.getCode()).isEqualTo(409);
    assertThat(duplicate.getResponseBody()).contains("data-eng");

    // A rename keeps the external id and the members.
    group =
        executeCliCommand(
            serverConfig, List.of("group", "update", "--id", id, "--name", "data-platform"));
    assertThat(group.get("name").asText()).isEqualTo("data-platform");
    assertThat(group.get("external_id").asText()).isEqualTo("okta-1");
    assertThat(memberIds(group)).containsExactly(bob);

    // Get and list, with and without a filter.
    group = executeCliCommand(serverConfig, List.of("group", "get", "--id", id));
    assertThat(group.get("name").asText()).isEqualTo("data-platform");
    assertThat(memberIds(group)).containsExactly(bob);
    assertThat(executeCliCommand(serverConfig, List.of("group", "list")))
        .extracting(g -> g.get("name").asText())
        .containsExactly("data-platform", "empty");
    assertThat(
            executeCliCommand(
                serverConfig, List.of("group", "list", "--filter", "displayName eq \"empty\"")))
        .extracting(g -> g.get("name").asText())
        .containsExactly("empty");
    assertThat(
            executeCliCommand(
                serverConfig, List.of("group", "list", "--start_index", "2", "--count", "1")))
        .extracting(g -> g.get("name").asText())
        .containsExactly("empty");

    // Delete; the group is gone and its members stay.
    executeCliCommand(serverConfig, List.of("group", "delete", "--id", id));
    ApiException gone = refusedCommand(List.of("group", "get", "--id", id));
    assertThat(gone.getCode()).isEqualTo(404);
    assertThat(gone.getResponseBody()).contains("Group not found: " + id);
    assertThat(executeCliCommand(serverConfig, List.of("user", "get", "--id", bob))).isNotNull();

    serverConfig.setAuthToken("");
  }

  @Test
  public void testGroupFailures() throws IOException {
    serverConfig.setAuthToken(Files.readString(Path.of("etc", "conf", "token.txt")));
    String ada = createUserId("ada@localhost");
    String id =
        executeCliCommand(serverConfig, List.of("group", "create", "--name", "data-eng"))
            .get("id")
            .asText();
    String unknownUser = UUID.randomUUID().toString();

    // A member that is no user is refused and nothing is created.
    ApiException refused =
        refusedCommand(List.of("group", "create", "--name", "ghosts", "--members", unknownUser));
    assertThat(refused.getCode()).isEqualTo(400);
    assertThat(refused.getResponseBody())
        .contains("Member is not an existing user: " + unknownUser);
    assertThat(
            refusedCommand(List.of("group", "add-member", "--id", id, "--member", unknownUser))
                .getCode())
        .isEqualTo(400);
    assertThat(executeCliCommand(serverConfig, List.of("group", "get", "--id", id)).get("members"))
        .isEmpty();

    // An unknown group is 404 on every command that names one.
    String unknownGroup = UUID.randomUUID().toString();
    for (List<String> args :
        List.of(
            List.of("group", "get", "--id", unknownGroup),
            List.of("group", "update", "--id", unknownGroup, "--name", "x"),
            List.of("group", "delete", "--id", unknownGroup),
            List.of("group", "add-member", "--id", unknownGroup, "--member", ada),
            List.of("group", "remove-member", "--id", unknownGroup, "--member", ada))) {
      assertThat(refusedCommand(args).getCode()).as(String.join(" ", args)).isEqualTo(404);
    }

    // A rename onto another group's name.
    executeCliCommand(serverConfig, List.of("group", "create", "--name", "other"));
    assertThat(refusedCommand(List.of("group", "update", "--id", id, "--name", "other")).getCode())
        .isEqualTo(409);

    serverConfig.setAuthToken("");
  }
}
