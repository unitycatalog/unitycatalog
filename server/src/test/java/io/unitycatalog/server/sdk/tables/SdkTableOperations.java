package io.unitycatalog.server.sdk.tables;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class SdkTableOperations implements TableOperations {
  private final TablesApi tablesApi;

  public SdkTableOperations(ApiClient apiClient) {
    this.tablesApi = new TablesApi(apiClient);
  }

  public TableInfo createTable(CreateTable createTableRequest) throws ApiException {
    if (createTableRequest.getTableType() == TableType.MANAGED) {
      // Caller actually should not set storage location for MANAGED tables. This function will set
      // it to the staging location of staging table.
      assert createTableRequest.getStorageLocation() == null;
      CreateStagingTable createStagingTableRequest =
          new CreateStagingTable()
              .catalogName(createTableRequest.getCatalogName())
              .schemaName(createTableRequest.getSchemaName())
              .name(createTableRequest.getName());
      StagingTableInfo stagingTableInfo = tablesApi.createStagingTable(createStagingTableRequest);
      createTableRequest.setStorageLocation(stagingTableInfo.getStagingLocation());
      // MANAGED Delta tables must carry the staging UUID in properties[UC_TABLE_ID]; the server
      // cross-checks this against the staging table's actual UUID to catch buggy/malicious
      // clients claiming the wrong table id. The test helper injects it so individual test
      // fixtures don't have to thread the staging UUID through manually.
      Map<String, String> props =
          createTableRequest.getProperties() != null
              ? new HashMap<>(createTableRequest.getProperties())
              : new HashMap<>();
      props.put(TableProperties.UC_TABLE_ID, stagingTableInfo.getId());
      createTableRequest.setProperties(props);
    }
    return tablesApi.createTable(createTableRequest);
  }

  @Override
  public List<TableInfo> listTables(
      String catalogName, String schemaName, Optional<String> pageToken) throws ApiException {
    return Objects.requireNonNull(
        tablesApi.listTables(catalogName, schemaName, 100, pageToken.orElse(null)).getTables());
  }

  @Override
  public TableInfo getTable(String tableFullName) throws ApiException {
    return tablesApi.getTable(
        tableFullName,
        /* readStreamingTableAsManaged = */ true,
        /* readMaterializedViewAsManaged = */ true);
  }

  @Override
  public void deleteTable(String tableFullName) throws ApiException {
    tablesApi.deleteTable(tableFullName);
  }
}
