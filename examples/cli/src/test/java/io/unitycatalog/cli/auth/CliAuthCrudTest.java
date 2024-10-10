package io.unitycatalog.cli.auth;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.auth0.jwt.JWT;
import com.fasterxml.jackson.databind.JsonNode;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class CliAuthCrudTest extends BaseAuthCRUDTest {

  @Test
  public void testAuthLoginExchange() throws IOException {
    System.out.println("Testing login exchange..");

    // Test exchange with server's access token
    // TODO: should really use an identity token, but this works for now
    Path path = Path.of("etc", "conf", "token.txt");
    String token = Files.readString(path);

    List<String> argsList = List.of("auth", "login", "--identity_token", token);

    String[] args = addServerAndAuthParams(argsList, serverConfig);
    JsonNode authExchangeInfo = executeCLICommand(args);

    assertThat(authExchangeInfo.get("access_token")).isNotNull();
  }

  @Test
  public void testAuthAccess()
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    System.out.println("Testing access..");

    List<String> argsList = List.of("catalog", "list");

    // Test with no Authentication on authenticated end point
    assertThatThrownBy(
            () -> {
              String[] localArgs = addServerAndAuthParams(argsList, serverConfig);
              executeCLICommand(localArgs);
            })
        .isInstanceOf(RuntimeException.class);

    // Test with Authentication on authenticated end point
    Path path = Path.of("etc", "conf", "token.txt");
    String token = Files.readString(path);

    serverConfig.setAuthToken(token);

    String[] args = addServerAndAuthParams(argsList, serverConfig);
    JsonNode responseJsonInfo = executeCLICommand(args);
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
              String[] localArgs = addServerAndAuthParams(argsList, serverConfig);
              executeCLICommand(localArgs);
            })
        .isInstanceOf(RuntimeException.class);

    serverConfig.setAuthToken(token);

    // Test adding a user
    List<String> argsAddUser =
        List.of("user", "create", "--email", "test@localhost", "--name", "Test User");
    args = addServerAndAuthParams(argsAddUser, serverConfig);
    responseJsonInfo = executeCLICommand(args);
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
    args = addServerAndAuthParams(argsList, serverConfig);
    responseJsonInfo = executeCLICommand(args);
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
    String[] args = addServerAndAuthParams(argsAddUser, serverConfig);
    JsonNode responseJsonInfo = executeCLICommand(args);
    assertThat(responseJsonInfo).isNotNull();
    assertThat(responseJsonInfo.get("name").asText()).isEqualTo("Test User");
    assertThat(responseJsonInfo.get("email").asText()).isEqualTo("user@localhost");

    String id = responseJsonInfo.get("id").asText();

    // Test creating a user that already exists.
    assertThatThrownBy(
            () -> {
              String[] localArgs = addServerAndAuthParams(argsAddUser, serverConfig);
              executeCLICommand(localArgs);
            })
        .isInstanceOf(RuntimeException.class);

    // Test updating a user
    List<String> argsUpdateUser =
        List.of(
            "user", "update", "--id", id, "--name", "Test User Updated", "--external_id", "123");
    args = addServerAndAuthParams(argsUpdateUser, serverConfig);
    responseJsonInfo = executeCLICommand(args);
    assertThat(responseJsonInfo).isNotNull();
    assertThat(responseJsonInfo.get("name").asText()).isEqualTo("Test User Updated");
    assertThat(responseJsonInfo.get("external_id").asText()).isEqualTo("123");

    // Test getting a user
    List<String> argsGetUser = List.of("user", "get", "--id", id);
    args = addServerAndAuthParams(argsGetUser, serverConfig);
    responseJsonInfo = executeCLICommand(args);
    assertThat(responseJsonInfo).isNotNull();
    assertThat(responseJsonInfo.get("name").asText()).isEqualTo("Test User Updated");

    // Test listing users
    List<String> argsListUsers = List.of("user", "list");
    args = addServerAndAuthParams(argsListUsers, serverConfig);
    responseJsonInfo = executeCLICommand(args);
    assertThat(responseJsonInfo).isNotNull();
    assertThat(responseJsonInfo).hasSize(2);

    // Test deleting a user
    List<String> argsDeleteUser = List.of("user", "delete", "--id", id);
    args = addServerAndAuthParams(argsDeleteUser, serverConfig);
    responseJsonInfo = executeCLICommand(args);
    assertThat(responseJsonInfo).isNotNull();

    serverConfig.setAuthToken("");
  }
}
