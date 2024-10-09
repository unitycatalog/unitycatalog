package io.unitycatalog.server.sdk.tables;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.StagingTablesApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.*;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Objects;

public class SdkTableOperations implements TableOperations {

  private final TablesApi tablesApi;

  private final StagingTablesApi stagingTablesApi;

  public SdkTableOperations(ApiClient apiClient) {
    this.tablesApi = new TablesApi(apiClient);
    this.stagingTablesApi = new StagingTablesApi(apiClient);
  }

  public TableInfo createTable(CreateTable createTableRequest) throws ApiException {
    if (createTableRequest.getTableType().equals(TableType.MANAGED)) {
      // Create a staging table
      CreateStagingTable createStagingTableRequest =
          new CreateStagingTable()
              .catalogName(createTableRequest.getCatalogName())
              .schemaName(createTableRequest.getSchemaName())
              .name(createTableRequest.getName());
      StagingTableInfo stagingTableInfo =
          stagingTablesApi.createStagingTable(createStagingTableRequest);
      String stagingLocation = stagingTableInfo.getStagingLocation();
      createTableRequest.setStorageLocation(stagingLocation);
    }
    return tablesApi.createTable(createTableRequest);
  }

  @Override
  public List<TableInfo> listTables(String catalogName, String schemaName) throws ApiException {
    return TestUtils.toList(
        Objects.requireNonNull(
            tablesApi.listTables(catalogName, schemaName, 100, null).getTables()));
  }

  @Override
  public TableInfo getTable(String tableFullName) throws ApiException {
    return tablesApi.getTable(tableFullName);
  }

  @Override
  public void deleteTable(String tableFullName) throws ApiException {
    tablesApi.deleteTable(tableFullName);
  }
}
