package io.unitycatalog.cli.access;

import static io.unitycatalog.cli.access.Step.Expect.FAIL;
import static io.unitycatalog.cli.access.Step.Expect.SUCCEED;

import io.unitycatalog.cli.access.Step.CommandStep;
import io.unitycatalog.cli.access.Step.TokenStep;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class CliAccessControlModelCrudTest extends CliAccessControlBaseCrudTest {

  List<Step> modelSteps =
      new ArrayList<>() {
        {
          addAll(commonUserSteps);
          addAll(commonSecurableSteps);

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  1,
                  "permission",
                  "create",
                  "--securable_type",
                  "schema",
                  "--name",
                  "cat_pr1.sch_pr1",
                  "--principal",
                  "principal-1@localhost",
                  "--privilege",
                  "USE SCHEMA"));
          add(
              CommandStep.of(
                  SUCCEED,
                  1,
                  "permission",
                  "create",
                  "--securable_type",
                  "schema",
                  "--name",
                  "cat_pr1.sch_pr1",
                  "--principal",
                  "principal-1@localhost",
                  "--privilege",
                  "CREATE MODEL"));
          add(
              CommandStep.of(
                  SUCCEED,
                  1,
                  "permission",
                  "create",
                  "--securable_type",
                  "schema",
                  "--name",
                  "cat_pr1.sch_pr1",
                  "--principal",
                  "principal-2@localhost",
                  "--privilege",
                  "USE SCHEMA"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "registered_model",
                  "create",
                  "--catalog",
                  "cat_pr1",
                  "--schema",
                  "sch_pr1",
                  "--name",
                  "mod_pr1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "registered_model", "get", "--full_name", "cat_pr1.sch_pr1.mod_pr1"));
          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL, "registered_model", "get", "--full_name", "cat_pr1.sch_pr1.mod_pr1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(SUCCEED, 1, "registered_model", "list"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(CommandStep.of(SUCCEED, 0, "registered_model", "list"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "registered_model",
                  "update",
                  "--full_name",
                  "cat_pr1.sch_pr1.mod_pr1",
                  "--comment",
                  "hello"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "registered_model",
                  "update",
                  "--full_name",
                  "cat_pr1.sch_pr1.mod_pr1",
                  "--comment",
                  "hello"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "model_version",
                  "create",
                  "--catalog",
                  "cat_pr1",
                  "--schema",
                  "sch_pr1",
                  "--name",
                  "mod_pr1",
                  "--source",
                  "model_source"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "model_version",
                  "create",
                  "--catalog",
                  "cat_pr1",
                  "--schema",
                  "sch_pr1",
                  "--name",
                  "mod_pr1",
                  "--source",
                  "model_source"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "model_version",
                  "get",
                  "--full_name",
                  "cat_pr1.sch_pr1.mod_pr1",
                  "--version",
                  "1"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "model_version",
                  "get",
                  "--full_name",
                  "cat_pr1.sch_pr1.mod_pr1",
                  "--version",
                  "1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, 1, "model_version", "list", "--full_name", "cat_pr1.sch_pr1.mod_pr1"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL, "model_version", "list", "--full_name", "cat_pr1.sch_pr1.mod_pr1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "model_version",
                  "update",
                  "--full_name",
                  "cat_pr1.sch_pr1.mod_pr1",
                  "--version",
                  "1",
                  "--comment",
                  "hello"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "model_version",
                  "update",
                  "--full_name",
                  "cat_pr1.sch_pr1.mod_pr1",
                  "--version",
                  "1",
                  "--comment",
                  "hello"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "model_version",
                  "delete",
                  "--full_name",
                  "cat_pr1.sch_pr1.mod_pr1",
                  "--version",
                  "1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "model_version",
                  "delete",
                  "--full_name",
                  "cat_pr1.sch_pr1.mod_pr1",
                  "--version",
                  "1"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL, "registered_model", "delete", "--full_name", "cat_pr1.sch_pr1.mod_pr1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "registered_model", "delete", "--full_name", "cat_pr1.sch_pr1.mod_pr1"));
        }
      };

  @Test
  public void testModelAccess()
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    System.out.println("Testing volume access..");
    testSteps(modelSteps);
  }
}
