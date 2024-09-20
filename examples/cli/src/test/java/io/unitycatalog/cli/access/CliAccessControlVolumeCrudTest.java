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

public class CliAccessControlVolumeCrudTest extends CliAccessControlBaseCrudTest {

  List<Step> volumeSteps =
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

          // give user USE CATALOG on cat_pr1
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
                  "regular-1@localhost",
                  "--privilege",
                  "USE CATALOG"));

          // give user CREATE SCHEMA on cat_pr1
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
                  "cat_pr1",
                  "--principal",
                  "regular-2@localhost",
                  "--privilege",
                  "USE CATALOG"));

          // create a schema (regular-2)
          add(TokenStep.of(SUCCEED, "regular-2@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "schema", "create", "--name", "sch_rg2", "--catalog", "cat_pr1"));

          // give user USE SCHEMA on sch_rg2
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
                  "cat_pr1.sch_rg2",
                  "--principal",
                  "regular-2@localhost",
                  "--privilege",
                  "USE SCHEMA"));

          // create volume (admin) -> metsstore admin -> allowed
          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
                  FAIL,
                  "volume",
                  "create",
                  "--full_name",
                  "cat_pr1.sch_rg2.vol_adm",
                  "--storage_location",
                  "/tmp/vol_adm"));

          // create volume (principal-1) -> owner [catalog], owner [schema] -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "volume",
                  "create",
                  "--full_name",
                  "cat_pr1.sch_pr1.vol_adm",
                  "--storage_location",
                  "/tmp/volume3"));

          // create volume (principal-1) -> owner [catalog], not owner "schema" -> denied
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "volume",
                  "create",
                  "--full_name",
                  "cat_pr1.sch_rg2.vol_adm",
                  "--storage_location",
                  "/tmp/volume3"));

          // list volume (admin) -> metastore admin -> allowed - "list" all
          add(TokenStep.of(SUCCEED, "admin"));
          add(
              CommandStep.of(
                  SUCCEED, 1, "volume", "list", "--catalog", "cat_pr1", "--schema", "sch_pr1"));

          // list volume (principal-1) -> owner (schema) -> allowed - filtered list
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, 1, "volume", "list", "--catalog", "cat_pr1", "--schema", "sch_pr1"));

          // get volume (principal-1) -> use catalog, use schema, owner [volume] -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(CommandStep.of(SUCCEED, "volume", "get", "--full_name", "cat_pr1.sch_pr1.vol_adm"));

          // get volume (admin) -> metastore admin -> allowed
          add(TokenStep.of(SUCCEED, "admin"));
          add(CommandStep.of(SUCCEED, "volume", "get", "--full_name", "cat_pr1.sch_pr1.vol_adm"));

          // get volume (regular-1) -> not read "volume" -> denied
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(CommandStep.of(FAIL, "volume", "get", "--full_name", "cat_pr1.sch_pr1.vol_adm"));

          // update volume (principal-1) -> catalog [owner], schema [owner], USE SCHEMA -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED,
                  "volume",
                  "update",
                  "--full_name",
                  "cat_pr1.sch_pr1.vol_adm",
                  "--comment",
                  "principal update"));

          // update volume (regular-1) -> not owner [catalog] -> denied
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(
              CommandStep.of(
                  FAIL,
                  "volume",
                  "update",
                  "--full_name",
                  "cat_pr1.sch_pr1.vol_adm",
                  "--comment",
                  "principal update"));

          // delete volume (regular-1) -> not owner [catalog] -> denied
          add(TokenStep.of(SUCCEED, "regular-1@localhost"));
          add(CommandStep.of(FAIL, "volume", "delete", "--full_name", "cat_pr1.sch_pr1.vol_adm"));

          // delete volume (principal-1) -> owner -> allowed
          add(TokenStep.of(SUCCEED, "principal-1@localhost"));
          add(
              CommandStep.of(
                  SUCCEED, "volume", "delete", "--full_name", "cat_pr1.sch_pr1.vol_adm"));
        }
      };

  @Test
  public void testVolumeAccess()
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    System.out.println("Testing volume access..");
    testSteps(volumeSteps);
  }
}
