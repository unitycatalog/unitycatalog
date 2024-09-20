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

public class CliAccessControlFunctionCrudTest extends CliAccessControlBaseCrudTest {

  List<Step> functionSteps =
      new ArrayList<>() {
        {
          addAll(commonUserSteps);
          addAll(commonSecurableSteps);

          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
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

          add(TokenStep.of(SUCCEED, "admin"));
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

          // create function -> use catalog, use "schema" -> allow
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "function",
                  "create",
                  "--full_name",
                  "cat_pr1.sch_pr1.fun_pr1",
                  "--data_type",
                  "INT",
                  "--input_params",
                  "param1 INT"));

          // create function -> no use catalog, no use "schema" -> deny
          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "function",
                  "create",
                  "--full_name",
                  "cat_pr1.schema3.fun_pr1",
                  "--data_type",
                  "INT",
                  "--input_params",
                  "param1 INT"));

          // list functions -> no privileges -> allow - filtered list
          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, 0, "function", "list", "--catalog", "cat_pr1", "--schema", "sch_pr1"));

          // list functions -> owner -> allow - filtered
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, 1, "function", "list", "--catalog", "cat_pr1", "--schema", "sch_pr1"));

          // get function -> no privileges -> denied
          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(CommandStep.of(FAIL, "function", "get", "--full_name", "cat_pr1.sch_pr1.fun_pr1"));

          // get function -> owner -> allow
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(SUCCEED, "function", "get", "--full_name", "cat_pr1.sch_pr1.fun_pr1"));

          // delete function -> no privileges -> denied
          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(CommandStep.of(FAIL, "function", "delete", "--full_name", "cat_pr1.sch_pr1.fun_pr1"));

          // delete function -> owner -> allow
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "function", "delete", "--full_name", "cat_pr1.sch_pr1.fun_pr1"));
        }
      };

  @Test
  public void testFunctionAccess()
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    System.out.println("Testing function access..");
    testSteps(functionSteps);
  }
}
