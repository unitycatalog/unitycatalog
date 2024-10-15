package io.unitycatalog.cli.access;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;
import static io.unitycatalog.cli.access.Step.Expect.FAIL;
import static io.unitycatalog.cli.access.Step.Expect.SUCCEED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class CliAccessControlBaseCrudTest extends BaseAccessControlCRUDTest {

  protected List<Step> commonUserSteps =
          new ArrayList<>() {
            {
              add(Step.TokenStep.of(SUCCEED, "admin"));
              add(
                      Step.CommandStep.of(
                              SUCCEED,
                              "user",
                              "create",
                              "--name",
                              "Principal 1",
                              "--email",
                              "principal-1@localhost"));
              add(
                      Step.CommandStep.of(
                              SUCCEED,
                              "user",
                              "create",
                              "--name",
                              "Principal 2",
                              "--email",
                              "principal-2@localhost"));
              add(
                      Step.CommandStep.of(
                              SUCCEED,
                              "user",
                              "create",
                              "--name",
                              "Regular 1",
                              "--email",
                              "regular-1@localhost"));
              add(
                      Step.CommandStep.of(
                              SUCCEED,
                              "user",
                              "create",
                              "--name",
                              "Regular 2",
                              "--email",
                              "regular-2@localhost"));
            }
          };

  protected List<Step> commonSecurableSteps =
          new ArrayList<>() {
            {
              // give user CREATE CATALOG
              add(Step.TokenStep.of(SUCCEED, "admin"));
              add(
                      Step.CommandStep.of(
                              SUCCEED,
                              1,
                              "permission",
                              "create",
                              "--securable_type",
                              "metastore",
                              "--name",
                              "metastore",
                              "--principal",
                              "principal-1@localhost",
                              "--privilege",
                              "CREATE CATALOG"));

              // create a catalog -> CREATE CATALOG -> allowed
              add(Step.TokenStep.of(SUCCEED, "principal-1@localhost"));
              add(
                      Step.CommandStep.of(
                              SUCCEED,
                              "catalog",
                              "create",
                              "--name",
                              "cat_pr1",
                              "--comment",
                              "(created from scratch)"));

              // give user CREATE SCHEMA on cat_pr1
              add(
                      Step.CommandStep.of(
                              SUCCEED,
                              1,
                              "permission",
                              "create",
                              "--securable_type",
                              "catalog",
                              "--name",
                              "cat_pr1",
                              "--principal",
                              "principal-1@localhost",
                              "--privilege",
                              "CREATE SCHEMA"));
              add(
                      Step.CommandStep.of(
                              SUCCEED,
                              1,
                              "permission",
                              "create",
                              "--securable_type",
                              "catalog",
                              "--name",
                              "cat_pr1",
                              "--principal",
                              "principal-1@localhost",
                              "--privilege",
                              "USE CATALOG"));

              add(Step.TokenStep.of(SUCCEED, "principal-1@localhost"));
              add(
                      Step.CommandStep.of(
                              SUCCEED, "schema", "create", "--name", "sch_pr1", "--catalog", "cat_pr1"));

            }
          };




  public void testSteps(List<Step> steps) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    Path path = Path.of("etc", "conf", "token.txt");
    String adminToken = Files.readString(path);
    String token = adminToken;

    int stepNumber = 0;
    for (Step step : steps) {
      stepNumber++;
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

        System.err.format("%3d Expect: %s set principal to %s\n", stepNumber, tokenStep.getExpectedResult(), tokenStep.getEmail());

      } else if (step instanceof Step.CommandStep commandStep) {
        serverConfig.setAuthToken(token);
        String[] args = addServerAndAuthParams(commandStep.getArgs(), serverConfig);

        System.err.format("%3d Expect: %s executing %s\n", stepNumber, commandStep.getExpectedResult(), commandStep.getArgs());

        if (commandStep.getExpectedResult() == FAIL) {
          assertThatThrownBy(() -> executeCLICommand(args)).isInstanceOf(RuntimeException.class);
        } else {
          JsonNode resultJson = executeCLICommand(args);
          assertThat(resultJson).isNotNull();
          if (resultJson.isArray()) {
            assertThat(resultJson).hasSize(step.getItemCount());
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
