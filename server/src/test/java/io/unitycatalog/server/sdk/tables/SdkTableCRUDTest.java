package io.unitycatalog.server.sdk.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTest;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.junit.jupiter.api.Test;

public class SdkTableCRUDTest extends BaseTableCRUDTest {

  private final List<ColumnInfo> columns =
      List.of(
          new ColumnInfo()
              .name("test_column")
              .typeText("INTEGER")
              .typeJson("{\"type\": \"integer\"}")
              .typeName(ColumnTypeName.INT)
              .position(0)
              .nullable(true));

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig config) {
    localTablesApi = new TablesApi(TestUtils.createApiClient(config));
    return new SdkTableOperations(TestUtils.createApiClient(config));
  }

  /**
   * The SDK tests elide the `Response` object and directly return the model object. However,
   * clients can interact with the REST API directly, so its signature needs to be tested as well.
   *
   * <p>The tests below use a direct `TableAPI` client to inspect the response object.
   */
  private TablesApi localTablesApi;

  private StagingTableDAO getStagingTable(String tableId) {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      return session.get(StagingTableDAO.class, UUID.fromString(tableId));
    }
  }

  @Test
  public void testListTablesWithNoNextPageTokenShouldReturnNull() throws Exception {
    TableInfo testingTable =
        createTestingTable(
            TestUtils.TABLE_NAME,
            TableType.EXTERNAL,
            Optional.of(TestUtils.STORAGE_LOCATION),
            tableOperations);
    ListTablesResponse resp =
        localTablesApi.listTables(
            testingTable.getCatalogName(), testingTable.getSchemaName(), 100, null);
    assertThat(resp.getNextPageToken()).isNull();
    assertThat(resp.getTables())
        .hasSize(1)
        .first()
        .usingRecursiveComparison()
        .ignoringFields("columns", "storageLocation")
        .isEqualTo(testingTable);
  }

  @Test
  public void testListTablesWithNextPageTokenShouldReturnNextPageToken() throws Exception {
    List<TableInfo> testingTables = createMultipleTestingTables(11);
    ListTablesResponse resp =
        localTablesApi.listTables(
            testingTables.get(0).getCatalogName(), testingTables.get(0).getSchemaName(), 10, null);
    assertThat(resp.getNextPageToken()).isNotNull();
    assertThat(resp.getTables()).hasSize(10);
    // Check the next page has the last table
    ListTablesResponse nextPageResp =
        localTablesApi.listTables(
            testingTables.get(0).getCatalogName(),
            testingTables.get(0).getSchemaName(),
            10,
            resp.getNextPageToken());
    assertThat(nextPageResp.getNextPageToken()).isNull();
    assertThat(nextPageResp.getTables()).hasSize(1);
  }

  /**
   * Test the complete flow of creating a staging table and then creating a managed table using that
   * staging table's location.
   */
  @Test
  public void testStagingTableCreationAndManagedTableFromStagingLocation() throws Exception {
    String stagingTableName = "staging_test_table";

    // Step 1: Create a staging table
    CreateStagingTable createStagingTableRequest =
        new CreateStagingTable()
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .name(stagingTableName);

    StagingTableInfo stagingTableInfo =
        localTablesApi.createStagingTable(createStagingTableRequest);

    // Verify staging table info
    assertThat(stagingTableInfo).isNotNull();
    assertThat(stagingTableInfo.getName()).isEqualTo(stagingTableName);
    assertThat(stagingTableInfo.getCatalogName()).isEqualTo(TestUtils.CATALOG_NAME);
    assertThat(stagingTableInfo.getSchemaName()).isEqualTo(TestUtils.SCHEMA_NAME);
    assertThat(stagingTableInfo.getId()).isNotNull();
    assertThat(stagingTableInfo.getStagingLocation()).isNotNull();
    assertThat(stagingTableInfo.getStagingLocation())
        .isEqualTo("file:///tmp/ucroot/tables/" + stagingTableInfo.getId());

    // Step 2: Create a managed table that's not DELTA
    CreateTable createTableRequestNotDelta =
        new CreateTable()
            .name(stagingTableName)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(columns)
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.PARQUET)
            .storageLocation(stagingTableInfo.getStagingLocation())
            .comment("Table created from staging location");
    // This should fail with INVALID_ARGUMENT
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(() -> localTablesApi.createTable(createTableRequestNotDelta))
        .satisfies(
            ex ->
                assertThat(ex.getCode())
                    .isEqualTo(ErrorCode.INVALID_ARGUMENT.getHttpStatus().code()))
        .withMessageContaining("Managed table creation is only supported for Delta format");

    // Step 3: Create a managed table using the staging location
    CreateTable createTableRequest =
        new CreateTable()
            .name(stagingTableName)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(columns)
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(stagingTableInfo.getStagingLocation())
            .comment("Table created from staging location");

    TableInfo tableInfo = localTablesApi.createTable(createTableRequest);

    // Verify the table was created successfully
    assertThat(tableInfo).isNotNull();
    assertThat(tableInfo.getName()).isEqualTo(stagingTableName);
    assertThat(tableInfo.getTableType()).isEqualTo(TableType.MANAGED);
    assertThat(tableInfo.getDataSourceFormat()).isEqualTo(DataSourceFormat.DELTA);
    assertThat(tableInfo.getStorageLocation()).isEqualTo(stagingTableInfo.getStagingLocation());
    assertThat(tableInfo.getTableId()).isEqualTo(stagingTableInfo.getId());

    // Also verify that the staging table has been commited
    StagingTableDAO commitedStagingTableDAO = getStagingTable(tableInfo.getTableId());
    assertThat(commitedStagingTableDAO).isNotNull();
    assertThat(commitedStagingTableDAO.getStagingLocation()).isNotNull();
    assertThat(commitedStagingTableDAO.getStagingLocation())
        .isEqualTo("file:///tmp/ucroot/tables/" + stagingTableInfo.getId());
    assertThat(commitedStagingTableDAO.isStageCommitted()).isEqualTo(true);

    // Clean up
    tableOperations.deleteTable(
        TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + stagingTableName);

    // Step 4: Try to create another table using the same (now committed) staging location
    String secondTableName = "second_table_same_staging";
    CreateTable secondCreateTableRequest =
        new CreateTable()
            .name(secondTableName)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(columns)
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(stagingTableInfo.getStagingLocation())
            .comment("Second table from same staging location - should fail");

    // This should fail with FAILED_PRECONDITION (already committed)
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(() -> localTablesApi.createTable(secondCreateTableRequest))
        .satisfies(
            ex ->
                assertThat(ex.getCode())
                    .isEqualTo(ErrorCode.FAILED_PRECONDITION.getHttpStatus().code()))
        .withMessageContaining("already committed");
  }

  /**
   * Test that attempting to create a managed table from a non-existent staging location fails with
   * NOT_FOUND error.
   */
  @Test
  public void testManagedTableCreationFromNonExistentStagingLocationShouldFail() throws Exception {
    // Use a fake staging location that doesn't exist
    String fakeLocationUuid = "00000000-0000-0000-0000-000000000000";
    String fakeStagingLocation = "file:///tmp/ucroot/tables/" + fakeLocationUuid;

    CreateTable createTableRequest =
        new CreateTable()
            .name("table_from_nonexistent_staging")
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(columns)
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(fakeStagingLocation)
            .comment("Table from non-existent staging location - should fail");

    // This should fail with NOT_FOUND
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(() -> localTablesApi.createTable(createTableRequest))
        .satisfies(
            ex -> assertThat(ex.getCode()).isEqualTo(ErrorCode.NOT_FOUND.getHttpStatus().code()))
        .withMessageContaining("not found");
  }

  /** Test that attempting to create a duplicate staging table fails with ALREADY_EXISTS error. */
  @Test
  public void testDuplicateStagingTableCreationShouldFail() throws Exception {
    // Create an external table
    String externalTableName = "duplicate_table";
    CreateTable createTableRequest =
        new CreateTable()
            .name(externalTableName)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(columns)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation("file:///tmp/ucroot/tables/external_path")
            .comment("Table created from external location");
    TableInfo tableInfo = localTablesApi.createTable(createTableRequest);
    assertThat(tableInfo).isNotNull();

    // Also create the first staging table
    String stagingTableName = "duplicate_staging_table";
    CreateStagingTable createStagingTableRequest =
        new CreateStagingTable()
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .name(stagingTableName);
    StagingTableInfo stagingTableInfo =
        localTablesApi.createStagingTable(createStagingTableRequest);
    assertThat(stagingTableInfo).isNotNull();

    // Try to create another staging table with the same name
    CreateStagingTable duplicateRequest =
        new CreateStagingTable()
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .name(stagingTableName);
    // This should fail with ALREADY_EXISTS
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(() -> localTablesApi.createStagingTable(duplicateRequest))
        .satisfies(
            ex ->
                assertThat(ex.getCode()).isEqualTo(ErrorCode.ALREADY_EXISTS.getHttpStatus().code()))
        .withMessageContaining("already exists");

    // The same story if the duplicate name is an external table
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(
            () -> localTablesApi.createStagingTable(duplicateRequest.name("duplicate_table")))
        .satisfies(
            ex ->
                assertThat(ex.getCode()).isEqualTo(ErrorCode.ALREADY_EXISTS.getHttpStatus().code()))
        .withMessageContaining("already exists");
  }

  /**
   * Test that creating multiple staging tables with different names works correctly and each gets a
   * unique staging location.
   */
  @Test
  public void testMultipleStagingTableCreation() throws Exception {
    String stagingTable1 = "staging_table_1";
    String stagingTable2 = "staging_table_2";

    // Create first staging table
    CreateStagingTable request1 =
        new CreateStagingTable()
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .name(stagingTable1);
    StagingTableInfo stagingInfo1 = localTablesApi.createStagingTable(request1);

    // Create second staging table
    CreateStagingTable request2 =
        new CreateStagingTable()
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .name(stagingTable2);
    StagingTableInfo stagingInfo2 = localTablesApi.createStagingTable(request2);

    // Verify staging tables have unique IDs and locations
    assertThat(stagingInfo1.getId()).isNotEqualTo(stagingInfo2.getId());
    assertThat(stagingInfo1.getStagingLocation()).isNotEqualTo(stagingInfo2.getStagingLocation());
  }

  /** Test that staging table creation fails when the schema doesn't exist. */
  @Test
  public void testStagingTableCreationWithNonExistentSchemaShouldFail() throws Exception {
    CreateStagingTable createStagingTableRequest =
        new CreateStagingTable()
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName("nonexistent_schema")
            .name("staging_table");

    // This should fail with NOT_FOUND
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(() -> localTablesApi.createStagingTable(createStagingTableRequest))
        .satisfies(
            ex -> assertThat(ex.getCode()).isEqualTo(ErrorCode.NOT_FOUND.getHttpStatus().code()))
        .withMessageContaining("not found");
  }

  /** Test that staging table creation fails when the catalog doesn't exist. */
  @Test
  public void testStagingTableCreationWithNonExistentCatalogShouldFail() {
    CreateStagingTable createStagingTableRequest =
        new CreateStagingTable()
            .catalogName("nonexistent_catalog")
            .schemaName(TestUtils.SCHEMA_NAME)
            .name("staging_table");

    // This should fail with NOT_FOUND
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(() -> localTablesApi.createStagingTable(createStagingTableRequest))
        .satisfies(
            ex -> assertThat(ex.getCode()).isEqualTo(ErrorCode.NOT_FOUND.getHttpStatus().code()))
        .withMessageContaining("not found");
  }
}
