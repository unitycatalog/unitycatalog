package io.unitycatalog.cli.auth;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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

    assertNotNull(authExchangeInfo.get("access_token"));
  }

  @Test
  public void testAuthAccess() throws IOException {
    System.out.println("Testing access..");

    List<String> argsList = List.of("catalog", "list");

    // Test with no Authentication on authenticated end point
    assertThrows(
        RuntimeException.class,
        () -> {
          String[] args = addServerAndAuthParams(argsList, serverConfig);
          JsonNode catalogInfoJson = executeCLICommand(args);
        });

    // Test with Authentication on authenticated end point
    Path path = Path.of("etc", "conf", "token.txt");
    String token = Files.readString(path);

    serverConfig.setAuthToken(token);

    String[] args = addServerAndAuthParams(argsList, serverConfig);
    JsonNode catalogInfoJson = executeCLICommand(args);
    assertNotNull(catalogInfoJson);
  }
}
