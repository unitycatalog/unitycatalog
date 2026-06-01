package io.unitycatalog.server.sdk.delta;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.DeltaCommitsApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.DeltaCommit;
import io.unitycatalog.client.model.DeltaGetCommits;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.delta.DeltaBaseTableCRUDTestEnv;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * End-to-end narrative for {@code server.managed-table.use-delta-api-only=true}: walk the full
 * MANAGED Delta lifecycle through the gated UC REST endpoints (each must reject) while their Delta
 * REST counterparts succeed, then prove the gate doesn't over-block by exercising the same UC
 * endpoints against an EXTERNAL table.
 *
 * <p>The flag-OFF default-path is not asserted here -- it is covered by every other UC REST CRUD /
 * commits test in this module, all of which run with the flag at its default ({@code false}).
 */
public class SdkUseDeltaApiOnlyFlagTest extends DeltaBaseTableCRUDTestEnv {

  private TablesApi ucTablesApi;
  private TemporaryCredentialsApi ucCredsApi;
  private DeltaCommitsApi ucCommitsApi;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig serverConfig) {
    // The flag test exercises UC's createTable on EXTERNAL Delta via the inherited
    // BaseTableCRUDTestEnv.createTestingTable helper, which goes through TableOperations.
    return new SdkTableOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(
        ServerProperties.Property.MANAGED_TABLE_USE_DELTA_API_ONLY.getKey(), "true");
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    ApiClient apiClient = TestUtils.createApiClient(serverConfig);
    ucTablesApi = new TablesApi(apiClient);
    ucCredsApi = new TemporaryCredentialsApi(apiClient);
    ucCommitsApi = new DeltaCommitsApi(apiClient);
  }

  @Test
  public void testManagedTablesUseDeltaApiOnlyFlag() throws ApiException {
    String tableName = "tbl_managed";

    // 1. UC createStagingTable -- blocked: staging tables always become MANAGED Delta.
    assertGateBlocks(
        () ->
            ucTablesApi.createStagingTable(
                new CreateStagingTable()
                    .name(tableName)
                    .catalogName(TestUtils.CATALOG_NAME)
                    .schemaName(TestUtils.SCHEMA_NAME)));

    // 2. Delta createStagingTable -- the only path that works under this flag.
    DeltaStagingTableResponse staging = createDeltaStaging(tableName);

    // 2b. UC generateTemporaryTableCredentials against the unfinalized staging row -- blocked.
    // Exercises the staging-row branch of getStorageLocationForTableOrStagingTable, which
    // projects staging rows as MANAGED so the gate catches them too.
    assertGateBlocks(
        () ->
            ucCredsApi.generateTemporaryTableCredentials(
                new GenerateTemporaryTableCredential()
                    .tableId(staging.getTableId().toString())
                    .operation(TableOperation.READ_WRITE)));

    // 3. UC createTable against that staging location -- blocked because the request is MANAGED.
    assertGateBlocks(
        () ->
            ucTablesApi.createTable(
                new CreateTable()
                    .name(tableName)
                    .catalogName(TestUtils.CATALOG_NAME)
                    .schemaName(TestUtils.SCHEMA_NAME)
                    .tableType(TableType.MANAGED)
                    .dataSourceFormat(DataSourceFormat.DELTA)
                    .storageLocation(staging.getLocation())));

    // 4. Delta createTable finalizes the staging table into a MANAGED Delta table.
    Handle managed = createDeltaManaged(tableName, staging, Map.of());

    // 5. UC getTable on the MANAGED Delta table -- allowed for now as non-Delta clients and
    // older clients supporting only EXTERNAL tables still needs to gracefully recognize MANAGED
    // Delta tables by loading them.
    ucTablesApi.getTable(fullName(tableName), null, null);

    // 6. UC generateTemporaryTableCredentials on the MANAGED Delta table -- blocked.
    assertGateBlocks(
        () ->
            ucCredsApi.generateTemporaryTableCredentials(
                new GenerateTemporaryTableCredential()
                    .tableId(managed.tableId().toString())
                    .operation(TableOperation.READ_WRITE)));

    // 7. UC postCommit -- blocked unconditionally (endpoint is MANAGED-Delta-only by contract).
    assertGateBlocks(
        () -> ucCommitsApi.commit(new DeltaCommit().tableId(managed.tableId().toString())));

    // 8. UC getCommits -- blocked unconditionally for the same reason.
    assertGateBlocks(
        () -> ucCommitsApi.getCommits(new DeltaGetCommits().tableId(managed.tableId().toString())));

    // 9. UC createTable EXTERNAL Delta -- allowed: the gate is precise to MANAGED. Goes through
    // BaseTableCRUDTestEnv.createTestingTable which uses the inherited UC TableOperations.
    TableInfo external =
        createTestingTable(
            "tbl_external",
            TableType.EXTERNAL,
            Optional.of(testDirectoryRoot.toString()),
            tableOperations);

    // 10. UC getTable on the EXTERNAL table -- allowed.
    ucTablesApi.getTable(fullName(external.getName()), null, null);

    // 11. UC generateTemporaryTableCredentials on the EXTERNAL table -- allowed.
    ucCredsApi.generateTemporaryTableCredentials(
        new GenerateTemporaryTableCredential()
            .tableId(external.getTableId())
            .operation(TableOperation.READ_WRITE));
  }

  /** Assert the request fails with INVALID_ARGUMENT and the gate's distinctive error wording. */
  private static void assertGateBlocks(Executable executable) {
    TestUtils.assertApiException(
        executable, ErrorCode.INVALID_ARGUMENT, "server.managed-table.use-delta-api-only=true");
  }

  private static String fullName(String tableName) {
    return TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + tableName;
  }
}
