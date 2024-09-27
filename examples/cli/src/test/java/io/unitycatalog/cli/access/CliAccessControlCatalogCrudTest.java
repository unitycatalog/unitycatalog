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

public class CliAccessControlCatalogCrudTest extends CliAccessControlBaseCrudTest {

  List<Step> catalogSteps =
      new ArrayList<>() {
        {
          addAll(commonUserSteps);
          // create a catalog -> metastore admin -> allowed
          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "catalog",
                  "create",
                  "--name",
                  "admincatalog1",
                  "--comment",
                  "(created from scratch)"));

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

          // create a catalog -> -- -> denied
          add(TokenStep.of(SUCCEED, "principal-2@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "catalog",
                  "create",
                  "--name",
                  "catalog2",
                  "--comment",
                  "(created from scratch)"));

          // list catalogs (admin) -> metastore admin -> allowed - list all
          add(TokenStep.of(SUCCEED, "admin"));
          add(CommandStep.of(SUCCEED, 2, "catalog", "list"));

          // list catalogs (principal-1) -> owner -> allowed - list owning
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(SUCCEED, 1, "catalog", "list"));

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

          // list catalogs (regular-1) -> USE CATALOG -> allowed - list filtered
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(CommandStep.of(SUCCEED, 1, "catalog", "list"));

          // get catalog (admin) should be able to get any catalog
          add(TokenStep.of(SUCCEED, "admin"));
          add(CommandStep.of(SUCCEED, "catalog", "get", "--name", "catalog1"));

          // get catalog (principal-1) -> denied
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(FAIL, "catalog", "get", "--name", "admincatalog1"));

          // get catalog (regular-1) -> USE CATALOG -> allowed
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(CommandStep.of(SUCCEED, "catalog", "get", "--name", "catalog1"));

          // get catalog (regular-1) -> denied
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(CommandStep.of(FAIL, "catalog", "get", "--name", "admincatalog1"));

          // update catalog (admin) -> metastore admin -> denied
          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
                  FAIL, "catalog", "update", "--name", "catalog1", "--comment", "(admin update)"));

          // update catalog (principal-1) -> owner -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "catalog",
                  "update",
                  "--name",
                  "catalog1",
                  "--comment",
                  "(principal update 1)"));

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

          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "catalog",
                  "update",
                  "--name",
                  "catalog1",
                  "--comment",
                  "(principal update 2)"));

          // update catalog (regular-1) -> use catalog -> denied
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "catalog",
                  "update",
                  "--name",
                  "catalog1",
                  "--comment",
                  "(regular update)"));

          // create a catalog -> metastore admin -> allowed
          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "catalog",
                  "create",
                  "--name",
                  "admincatalog2",
                  "--comment",
                  "(created from scratch)"));

          // delete a catalog -> denied
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(FAIL, "catalog", "delete", "--name", "admincatalog2"));

          // delete a catalog -> metastore admin -> allowed
          add(TokenStep.of(SUCCEED, "admin"));
          add(CommandStep.of(SUCCEED, "catalog", "delete", "--name", "admincatalog2"));

          // create a catalog -> CREATE CATALOG -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "catalog",
                  "create",
                  "--name",
                  "catalog2",
                  "--comment",
                  "(created from scratch)"));

          // delete a catalog -> metastore admin -> allowed
          add(TokenStep.of(SUCCEED, "admin"));
          add(CommandStep.of(SUCCEED, "catalog", "delete", "--name", "catalog2"));

          // create a catalog -> CREATE CATALOG -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "catalog",
                  "create",
                  "--name",
                  "catalog3",
                  "--comment",
                  "(created from scratch)"));

          // delete a catalog -> owner -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(SUCCEED, "catalog", "delete", "--name", "catalog3"));
        }
      };

  @Test
  public void testCatalogAccess()
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    System.out.println("Testing catalog access..");
    testSteps(catalogSteps);
  }
}
