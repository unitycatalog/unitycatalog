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

public class CliAccessControlTableCrudTest extends CliAccessControlBaseCrudTest {

  List<Step> tableSteps =
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
                  "regular-1@localhost",
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

          // create table (principal-1) -> owner, use catalog, USE SCHEMA -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "table",
                  "create",
                  "--full_name",
                  "catalog1.schema2.table1",
                  "--columns",
                  "id INT",
                  "--storage_location",
                  "/tmp/table1"));

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
                  "regular-1@localhost",
                  "--privilege",
                  "USE SCHEMA"));

          // create table (regular-1) -> not owner, use catalog, USE SCHEMA -> allowed
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "table",
                  "create",
                  "--full_name",
                  "catalog1.schema2.table2",
                  "--columns",
                  "id INT",
                  "--storage_location",
                  "/tmp/table2"));

          // list tables (admin) -> metastore admin -> allowed - list all
          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
                  SUCCEED, 2, "table", "list", "--catalog", "catalog1", "--schema", "schema2"));

          // list tables (principal-1) -> owner -> allowed -> filtered list
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, 2, "table", "list", "--catalog", "catalog1", "--schema", "schema2"));

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
                  "regular-2@localhost",
                  "--privilege",
                  "USE SCHEMA"));

          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
                  SUCCEED,
                  1,
                  "permission",
                  "create",
                  "--securable_type",
                  "table",
                  "--name",
                  "catalog1.schema2.table1",
                  "--principal",
                  "regular-2@localhost",
                  "--privilege",
                  "SELECT"));

          // list tables (principal-3) -> use catalog, use schema, select -> allowed -> filtered
          add(TokenStep.of(SUCCEED, "regular-2@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, 1, "table", "list", "--catalog", "catalog1", "--schema", "schema2"));

          // list tables (regular-1) -> -- -> empty list
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, 1, "table", "list", "--catalog", "catalog1", "--schema", "schema2"));

          // get, table (admin) -> metastore admin -> allowed
          add(TokenStep.of(SUCCEED, "admin"));
          add(CommandStep.of(SUCCEED, "table", "get", "--full_name", "catalog1.schema2.table1"));

          // get, table (principal-1) -> owner [catalog] -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(SUCCEED, "table", "get", "--full_name", "catalog1.schema2.table1"));

          // get, table (regular-2) -> use schema, use catalog, select [table] -> allowed
          add(TokenStep.of(SUCCEED, "regular-2@localhost"));
          add(CommandStep.of(SUCCEED, "table", "get", "--full_name", "catalog1.schema2.table1"));

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

          add(TokenStep.of(SUCCEED, "regular-2@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "schema", "create", "--name", "schema3", "--catalog", "catalog1"));

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

          // create, table (regular-2) -> owner [schema], USE CATALOG -> allowed
          add(TokenStep.of(SUCCEED, "regular-2@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "table",
                  "create",
                  "--full_name",
                  "catalog1.schema3.table3",
                  "--columns",
                  "id INT",
                  "--storage_location",
                  "/tmp/table3"));

          // delete table (regular-1) -> -- -> denied
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(CommandStep.of(FAIL, "table", "delete", "--full_name", "catalog1.schema3.table3"));

          // delete table (principal-1) -> owner [catalog], not owner [schema] -> denied
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(FAIL, "table", "delete", "--full_name", "catalog1.schema3.table3"));

          // delete table (regular-2) -> owner [schema], owner [table] -> allow
          add(TokenStep.of(SUCCEED, "regular-2@localhost"));
          add(CommandStep.of(SUCCEED, "table", "delete", "--full_name", "catalog1.schema3.table3"));
        }
      };

  @Test
  public void testTableAccess()
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    System.out.println("Testing table access..");
    testSteps(tableSteps);
  }
}
