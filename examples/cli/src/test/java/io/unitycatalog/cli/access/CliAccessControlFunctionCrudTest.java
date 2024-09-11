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
          // create a user (principal-1)
          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "user",
                  "create",
                  "--name",
                  "Principal 1",
                  "--email",
                  "principal-1@localhost"));

          // give user, CREATE CATALOG
          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
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
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "catalog",
                  "create",
                  "--name",
                  "catalog1",
                  "--comment",
                  "(created from scratch)"));

          // give user CREATE SCHEMA on catalog1
          add(
              CommandStep.of(
                  SUCCEED,
                  1,
                  "permission",
                  "create",
                  "--securable_type",
                  "catalog",
                  "--name",
                  "catalog1",
                  "--principal",
                  "principal-1@localhost",
                  "--privilege",
                  "CREATE SCHEMA"));
          add(
              CommandStep.of(
                  SUCCEED,
                  1,
                  "permission",
                  "create",
                  "--securable_type",
                  "catalog",
                  "--name",
                  "catalog1",
                  "--principal",
                  "principal-1@localhost",
                  "--privilege",
                  "USE CATALOG"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "schema", "create", "--name", "schema2", "--catalog", "catalog1"));

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
                  "catalog1",
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
                  "catalog1.schema2",
                  "--principal",
                  "principal-1@localhost",
                  "--privilege",
                  "USE SCHEMA"));

          // create a user (principal-2)
          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "user",
                  "create",
                  "--name",
                  "Principal 2",
                  "--email",
                  "principal-2@localhost"));

          // create function -> use catalog, use "schema" -> allow
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "function",
                  "create",
                  "--full_name",
                  "catalog1.schema2.function2",
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
                  "catalog1.schema3.function2",
                  "--data_type",
                  "INT",
                  "--input_params",
                  "param1 INT"));

          // list functions -> no privileges -> allow - filtered list
          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, 0, "function", "list", "--catalog", "catalog1", "--schema", "schema2"));

          // list functions -> owner -> allow - filtered
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, 1, "function", "list", "--catalog", "catalog1", "--schema", "schema2"));

          // get function -> no privileges -> denied
          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(CommandStep.of(FAIL, "function", "get", "--full_name", "catalog1.schema2.function2"));

          // get function -> owner -> allow
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "function", "get", "--full_name", "catalog1.schema2.function2"));

          // delete function -> no privileges -> denied
          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL, "function", "delete", "--full_name", "catalog1.schema2.function2"));

          // delete function -> owner -> allow
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "function", "delete", "--full_name", "catalog1.schema2.function2"));
        }
      };

  @Test
  public void testFunctionAccess()
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    System.out.println("Testing function access..");
    testSteps(functionSteps);
  }
}
