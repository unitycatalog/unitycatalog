package io.unitycatalog.cli.auth;

import com.auth0.jwt.JWT;
import com.fasterxml.jackson.databind.JsonNode;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    assertNotNull(authExchangeInfo.get("access_token"));
  }

  @Test
  public void testAuthAccess()
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    System.out.println("Testing access..");

    List<String> argsList = List.of("catalog", "list");

    // Test with no Authentication on authenticated end point
    assertThrows(
        RuntimeException.class,
        () -> {
          String[] localArgs = addServerAndAuthParams(argsList, serverConfig);
          JsonNode localResultJson = executeCLICommand(localArgs);
        });

    // Test with Authentication on authenticated end point
    Path path = Path.of("etc", "conf", "token.txt");
    String token = Files.readString(path);

    serverConfig.setAuthToken(token);

    String[] args = addServerAndAuthParams(argsList, serverConfig);
    JsonNode catalogInfoJson = executeCLICommand(args);
    assertNotNull(catalogInfoJson);

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

    assertThrows(
        RuntimeException.class,
        () -> {
          String[] localArgs = addServerAndAuthParams(argsList, serverConfig);
          JsonNode localResultJson = executeCLICommand(localArgs);
        });

    // Test adding a user
    List<String> argsAddUser =
        List.of("user", "create", "--email", "test@localhost", "--name", "Test User");
    args = addServerAndAuthParams(argsAddUser, serverConfig);
    catalogInfoJson = executeCLICommand(args);
    assertNotNull(catalogInfoJson);

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
    catalogInfoJson = executeCLICommand(args);
    assertNotNull(catalogInfoJson);
  }
}
