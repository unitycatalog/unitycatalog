package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * SDK-based access control tests for Table CRUD operations.
 *
 * <p>This test class verifies:
 *
 * <ul>
 *   <li>Table creation requires ownership or CREATE TABLE permission
 *   <li>Table listing is filtered based on permissions
 *   <li>Table get requires SELECT or ownership
 *   <li>Table delete requires ownership
 *   <li>Proper handling of USE CATALOG and USE SCHEMA permissions
 * </ul>
 */
public class SdkTableAccessControlCRUDTest extends SdkAccessControlBaseCRUDTest {

  @Test
  @SneakyThrows
  public void testTableAccess() {
    createCommonTestUsers();
    setupCommonCatalogAndSchema();

    // Create API clients for different users
    ServerConfig principal1Config = createTestUserServerConfig(PRINCIPAL_1);
    ServerConfig regular1Config = createTestUserServerConfig(REGULAR_1);
    ServerConfig regular2Config = createTestUserServerConfig("regular-2@localhost");

    TablesApi adminTablesApi = new TablesApi(adminApiClient);
    TablesApi principal1TablesApi = new TablesApi(TestUtils.createApiClient(principal1Config));
    TablesApi regular1TablesApi = new TablesApi(TestUtils.createApiClient(regular1Config));
    TablesApi regular2TablesApi = new TablesApi(TestUtils.createApiClient(regular2Config));
    SchemasApi regular2SchemasApi = new SchemasApi(TestUtils.createApiClient(regular2Config));

    // Grant USE CATALOG and USE SCHEMA to principal-1
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.USE_SCHEMA);

    // Grant USE CATALOG and CREATE SCHEMA to regular-1
    grantPermissions(REGULAR_1, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    grantPermissions(REGULAR_1, SecurableType.CATALOG, "cat_pr1", Privileges.CREATE_SCHEMA);

    // create table (principal-1) -> owner, use catalog, USE SCHEMA -> allowed
    CreateTable createTable1 =
        new CreateTable()
            .name("tbl_pr1")
            .catalogName("cat_pr1")
            .schemaName("sch_pr1")
            .columns(TEST_COLUMNS)
            .storageLocation("/tmp/tbl_pr1")
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA);
    TableInfo table1Info = principal1TablesApi.createTable(createTable1);
    assertThat(table1Info).isNotNull();
    assertThat(table1Info.getName()).isEqualTo("tbl_pr1");

    // Grant USE CATALOG and USE SCHEMA to regular-1
    grantPermissions(REGULAR_1, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.USE_SCHEMA);

    // TEST: Create table as regular-1 (not owner, no CREATE TABLE) - should fail
    CreateTable createTableRg1 =
        new CreateTable()
            .name("tab_rg1")
            .catalogName("cat_pr1")
            .schemaName("sch_pr1")
            .columns(TEST_COLUMNS)
            .storageLocation("/tmp/tab_rg1")
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA);
    assertPermissionDenied(() -> regular1TablesApi.createTable(createTableRg1));

    // grant CREATE TABLE permission to regular-1
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.CREATE_TABLE);

    // create table (regular-1) -> not owner, use catalog, USE SCHEMA, create table -> allowed
    TableInfo tableRg1Info = regular1TablesApi.createTable(createTableRg1);
    assertThat(tableRg1Info).isNotNull();
    assertThat(tableRg1Info.getName()).isEqualTo("tab_rg1");

    // list tables (admin) -> metastore admin -> allowed - list all
    List<TableInfo> adminTables = listAllTables(adminTablesApi, "cat_pr1", "sch_pr1");
    assertThat(adminTables).hasSize(2);

    // list tables (principal-1) -> owner -> allowed - list all
    List<TableInfo> principal1Tables = listAllTables(principal1TablesApi, "cat_pr1", "sch_pr1");
    assertThat(principal1Tables).hasSize(2);

    // grant permissions to regular-2
    grantPermissions(
        "regular-2@localhost", SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    grantPermissions(
        "regular-2@localhost", SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.USE_SCHEMA);
    grantPermissions(
        "regular-2@localhost", SecurableType.TABLE, "cat_pr1.sch_pr1.tbl_pr1", Privileges.SELECT);

    // list tables (regular-2) -> use catalog, use schema, select -> allowed -> filtered list
    List<TableInfo> regular2Tables = listAllTables(regular2TablesApi, "cat_pr1", "sch_pr1");
    assertThat(regular2Tables).hasSize(1);
    assertThat(regular2Tables.get(0).getName()).isEqualTo("tbl_pr1");

    // list tables (regular-1) -> -- -> owner table only
    List<TableInfo> regular1Tables = listAllTables(regular1TablesApi, "cat_pr1", "sch_pr1");
    assertThat(regular1Tables).hasSize(1);
    assertThat(regular1Tables.get(0).getName()).isEqualTo("tab_rg1");

    // get, table (admin) -> metastore admin -> allowed
    TableInfo tableInfoAdmin = getTable(adminTablesApi, "cat_pr1.sch_pr1.tbl_pr1");
    assertThat(tableInfoAdmin).isNotNull();

    // get, table (principal-1) -> owner [catalog] -> allowed
    TableInfo tableInfoOwner = getTable(principal1TablesApi, "cat_pr1.sch_pr1.tbl_pr1");
    assertThat(tableInfoOwner).isNotNull();

    // get, table (regular-2) -> use schema, use catalog, select [table] -> allowed
    TableInfo tableInfoRegular2 = getTable(regular2TablesApi, "cat_pr1.sch_pr1.tbl_pr1");
    assertThat(tableInfoRegular2).isNotNull();

    // grant CREATE SCHEMA to regular-2
    grantPermissions(
        "regular-2@localhost", SecurableType.CATALOG, "cat_pr1", Privileges.CREATE_SCHEMA);

    // create schema as regular-2
    CreateSchema createSchemaRg2 = new CreateSchema().name("sch_rg2").catalogName("cat_pr1");
    regular2SchemasApi.createSchema(createSchemaRg2);

    // grant USE SCHEMA to regular-2
    grantPermissions(
        "regular-2@localhost", SecurableType.SCHEMA, "cat_pr1.sch_rg2", Privileges.USE_SCHEMA);

    // create, table (regular-2) -> owner [schema], USE CATALOG -> allowed
    CreateTable createTableRg2 =
        new CreateTable()
            .name("tab_rg2")
            .catalogName("cat_pr1")
            .schemaName("sch_rg2")
            .columns(TEST_COLUMNS)
            .storageLocation("/tmp/tab_rg2")
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA);
    TableInfo tableRg2Info = regular2TablesApi.createTable(createTableRg2);
    assertThat(tableRg2Info).isNotNull();

    // delete table (regular-1) -> -- -> denied
    assertPermissionDenied(() -> regular1TablesApi.deleteTable("cat_pr1.sch_rg2.tab_rg2"));

    // delete table (principal-1) -> owner [catalog] -> allowed
    principal1TablesApi.deleteTable("cat_pr1.sch_rg2.tab_rg2");
  }
}
