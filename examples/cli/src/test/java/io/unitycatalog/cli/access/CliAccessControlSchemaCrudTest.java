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

public class CliAccessControlSchemaCrudTest extends CliAccessControlBaseCrudTest {

  List<Step> schemaSteps =
      new ArrayList<>() {
        {
          addAll(commonUserSteps);

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

          // give user USE CATALOG on catalog1
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
                  "regular-1@localhost",
                  "--privilege",
                  "USE CATALOG"));

          // create a schema (admin) -> metastore admin -> allowed
          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
                  SUCCEED, "schema", "create", "--name", "adminschema1", "--catalog", "catalog1"));

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
          // create a schema (principal-1) -> CREATE SCHEMA -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "schema", "create", "--name", "schema1", "--catalog", "catalog1"));

          // create a schema (regular-1) -> denied
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(
              CommandStep.of(
                  FAIL, "schema", "create", "--name", "schema2", "--catalog", "catalog1"));

          // give user CREATE SCHEMA on catalog1
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
                  "regular-2@localhost",
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
                  "regular-2@localhost",
                  "--privilege",
                  "USE CATALOG"));

          // create a schema (regular-2)
          add(TokenStep.of(SUCCEED, "regular-2@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "schema", "create", "--name", "schema3", "--catalog", "catalog1"));

          // give user USE SCHEMA on schema3
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
                  "catalog1.schema3",
                  "--principal",
                  "regular-2@localhost",
                  "--privilege",
                  "USE SCHEMA"));

          // list schemas (admin) -> metastore admin -> allowed - list all
          add(TokenStep.of(SUCCEED, "admin"));
          add(CommandStep.of(SUCCEED, 3, "schema", "list", "--catalog", "catalog1"));

          // list schemas (principal-1) -> owner (catalog) -> allowed - list all
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(SUCCEED, 3, "schema", "list", "--catalog", "catalog1"));

          // list schemas (regular-1) -> -> allowed - empty list
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(CommandStep.of(SUCCEED, 3, "schema", "list", "--catalog", "catalog1"));

          // list schemas (regular-2) -> -> USE SCHEMA - filtered list
          add(TokenStep.of(SUCCEED, "regular-2@localhost"));
          add(CommandStep.of(SUCCEED, 3, "schema", "list", "--catalog", "catalog1"));

          // get schema (admin) -> metastore admin -> allowed
          add(TokenStep.of(SUCCEED, "admin"));
          add(CommandStep.of(SUCCEED, "schema", "get", "--full_name", "catalog1.schema1"));

          // get schema (principal-1) -> owner -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(SUCCEED, "schema", "get", "--full_name", "catalog1.schema1"));

          // get schema (regular-1) -> -- -> denied
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(CommandStep.of(FAIL, "schema", "get", "--full_name", "catalog1.schema1"));

          // get schema (regular-1) -> USE SCHEMA -> allowed
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
                  "catalog1.schema1",
                  "--principal",
                  "regular-2@localhost",
                  "--privilege",
                  "USE SCHEMA"));

          add(TokenStep.of(SUCCEED, "regular-2@localhost"));
          add(CommandStep.of(SUCCEED, "schema", "get", "--full_name", "catalog1.schema1"));

          // update schema (admin) -> metastore admin -> allowed
          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "schema",
                  "update",
                  "--full_name",
                  "catalog1.schema1",
                  "--comment",
                  "(admin update)"));

          // update schema (principal-1) -> owner -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "schema",
                  "update",
                  "--full_name",
                  "catalog1.schema1",
                  "--comment",
                  "(principal update)"));

          // update schema (regular-1) -> -- -> denied
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "schema",
                  "update",
                  "--full_name",
                  "catalog1.schema1",
                  "--comment",
                  "(regular update)"));

          // delete schema (regular-1) -> -- -> denied
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(CommandStep.of(FAIL, "schema", "delete", "--full_name", "catalog1.schema1"));

          // delete schema (regular-1) -> "schema" owner -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(SUCCEED, "schema", "delete", "--full_name", "catalog1.schema1"));

          // delete schema (regular-1) -> "catalog" owner -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(FAIL, "schema", "delete", "--full_name", "catalog1.schema3"));

          add(TokenStep.of(SUCCEED, "regular-2@localhost"));
          add(CommandStep.of(SUCCEED, "schema", "delete", "--full_name", "catalog1.schema3"));
        }
      };

  @Test
  public void testSchemaAccess()
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    System.out.println("Testing schema access..");
    testSteps(schemaSteps);
  }
}
