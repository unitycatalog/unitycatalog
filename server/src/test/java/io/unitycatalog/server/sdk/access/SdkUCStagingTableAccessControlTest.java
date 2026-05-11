package io.unitycatalog.server.sdk.access;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;

/**
 * Runs the staging-table access-control suite against the UC REST createStagingTable / createTable.
 */
public class SdkUCStagingTableAccessControlTest extends SdkStagingTableAccessControlTest {

  private static final List<ColumnInfo> COLUMNS =
      List.of(
          new ColumnInfo()
              .name("test_column")
              .typeText("INTEGER")
              .typeJson("{\"type\": \"integer\"}")
              .typeName(ColumnTypeName.INT)
              .position(0)
              .nullable(true));

  @Override
  protected StagingHandle createStaging(
      ServerConfig config, String catalog, String schema, String name) throws Exception {
    StagingTableInfo info =
        new TablesApi(TestUtils.createApiClient(config))
            .createStagingTable(
                new CreateStagingTable().catalogName(catalog).schemaName(schema).name(name));
    return new StagingHandle(info.getId(), info.getStagingLocation());
  }

  @Override
  protected FinalizedTable finalizeManagedTable(
      ServerConfig config, StagingHandle staging, String name) throws Exception {
    TableInfo info =
        new TablesApi(TestUtils.createApiClient(config))
            .createTable(
                new CreateTable()
                    .name(name)
                    .catalogName(TestUtils.CATALOG_NAME)
                    .schemaName(TestUtils.SCHEMA_NAME)
                    .columns(COLUMNS)
                    .tableType(TableType.MANAGED)
                    .dataSourceFormat(DataSourceFormat.DELTA)
                    .storageLocation(staging.location())
                    .properties(Map.of(TableProperties.UC_TABLE_ID, staging.id())));
    return new FinalizedTable(info.getTableId(), info.getStorageLocation());
  }

  @Override
  protected void fetchTempCreds(ServerConfig config, String tableId) throws Exception {
    new TemporaryCredentialsApi(TestUtils.createApiClient(config))
        .generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential()
                .tableId(tableId)
                .operation(TableOperation.READ_WRITE));
  }
}
