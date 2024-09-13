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

          // create a user (principal-1)
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

          // give user CREATE CATALOG
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
                  "principal-2@localhost",
                  "--privilege",
                  "USE CATALOG"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "schema", "create", "--name", "schema1", "--catalog", "catalog1"));

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
                  "catalog1.schema1",
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
                  "catalog1.schema1",
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
                  "catalog1.schema1",
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
                  "catalog1",
                  "--schema",
                  "schema1",
                  "--name",
                  "model1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "registered_model", "get", "--full_name", "catalog1.schema1.model1"));
          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL, "registered_model", "get", "--full_name", "catalog1.schema1.model1"));

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
                  "catalog1.schema1.model1",
                  "--comment",
                  "hello"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "registered_model",
                  "update",
                  "--full_name",
                  "catalog1.schema1.model1",
                  "--comment",
                  "hello"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "model_version",
                  "create",
                  "--catalog",
                  "catalog1",
                  "--schema",
                  "schema1",
                  "--name",
                  "model1"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "model_version",
                  "create",
                  "--catalog",
                  "catalog1",
                  "--schema",
                  "schema1",
                  "--name",
                  "model1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "model_version",
                  "get",
                  "--full_name",
                  "catalog1.schema1.model1",
                  "--version",
                  "1"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "model_version",
                  "get",
                  "--full_name",
                  "catalog1.schema1.model1",
                  "--version",
                  "1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, 1, "model_version", "list", "--full_name", "catalog1.schema1.model1"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL, "model_version", "list", "--full_name", "catalog1.schema1.model1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "model_version",
                  "update",
                  "--full_name",
                  "catalog1.schema1.model1",
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
                  "catalog1.schema1.model1",
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
                  "catalog1.schema1.model1",
                  "--version",
                  "1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "model_version",
                  "delete",
                  "--full_name",
                  "catalog1.schema1.model1",
                  "--version",
                  "1"));

          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL, "registered_model", "delete", "--full_name", "catalog1.schema1.model1"));

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "registered_model", "delete", "--full_name", "catalog1.schema1.model1"));
        }
      };

  @Test
  public void testModelAccess()
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    System.out.println("Testing volume access..");
    testSteps(modelSteps);
  }
}
