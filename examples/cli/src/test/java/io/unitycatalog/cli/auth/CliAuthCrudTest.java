package io.unitycatalog.cli.auth;

import static io.unitycatalog.cli.TestUtils.executeCliCommand;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class CliAuthCrudTest extends BaseAuthCRUDTest {

  /**
   * Creates a signed identity token for token exchange tests.
   *
   * @param issuer the token issuer
   * @param audience the token audience (may be null)
   * @param algorithm the signing algorithm to use
   * @param keyId the key ID to include in the JWT header
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

  @Test
  public void testAuthLoginExchangeWithCorrectIssuerAndAudience() {
    System.out.println("Testing login exchange with correct issuer and audience..");

    String token =
        createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

    List<String> argsList = List.of("auth", "login", "--identity_token", token);

    JsonNode authExchangeInfo = executeCliCommand(serverConfig, argsList);
    assertThat(authExchangeInfo.get("access_token")).isNotNull();
  }

  @Test
  public void testAuthLoginExchangeWithCorrectIssuerAndWrongAudience() {
    System.out.println("Testing login exchange with correct issuer but wrong audience..");

    String token =
        createIdentityToken(testIssuer, "wrong-audience", testIssuerAlgorithm, testIssuerKeyId);

    List<String> argsList = List.of("auth", "login", "--identity_token", token);
    assertThatThrownBy(() -> executeCliCommand(serverConfig, argsList))
        .isInstanceOf(RuntimeException.class);
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
            "https://evil-issuer.example.com", TEST_AUDIENCE, foreignAlgorithm, foreignKeyId);

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
    Path path = Path.of("etc", "conf", "token.txt");
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
    Path path = Path.of("etc", "conf", "token.txt");
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
}
