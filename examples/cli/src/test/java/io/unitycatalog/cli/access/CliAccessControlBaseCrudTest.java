package io.unitycatalog.cli.access;

import com.auth0.jwt.JWT;
import com.fasterxml.jackson.databind.JsonNode;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.access.BaseAccessControlCRUDTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
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

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;
import static io.unitycatalog.cli.access.Step.Expect.FAIL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CliAccessControlBaseCrudTest extends BaseAccessControlCRUDTest {

  public void testSteps(List<Step> steps) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    Path path = Path.of("etc", "conf", "token.txt");
    String adminToken = Files.readString(path);
    String token = adminToken;

    for (Step step : steps) {
      if (step instanceof Step.TokenStep tokenStep) {
        token =
                JWT.create()
                        .withSubject(securityContext.getServiceName())
                        .withIssuer(securityContext.getLocalIssuer())
                        .withIssuedAt(new Date())
                        .withKeyId(securityConfiguration.getKeyId())
                        .withJWTId(UUID.randomUUID().toString())
                        .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.ACCESS.name())
                        .withClaim(JwtClaim.SUBJECT.key(), tokenStep.getEmail())
                        .sign(securityConfiguration.algorithmRSA());
      } else if (step instanceof Step.CommandStep commandStep) {
        serverConfig.setAuthToken(token);
        String[] args = addServerAndAuthParams(commandStep.getArgs(), serverConfig);

        System.err.println("Expect: " + commandStep.getExpectedResult() + " executing " + commandStep.getArgs());

        if (commandStep.getExpectedResult() == FAIL) {
          assertThrows(
                  RuntimeException.class,
                  () -> {
                    JsonNode localResultJson = executeCLICommand(args);
                  });
        } else {
          JsonNode resultJson = executeCLICommand(args);
          assertNotNull(resultJson);
          if (resultJson.isArray()) {
            assertEquals(step.getItemCount(), resultJson.size());
          }
        }
      }
    }

  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return null;
  }
}