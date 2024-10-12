package io.unitycatalog.server.sdk.tables;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.base.table.TableOperations;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SdkTableOperations implements TableOperations {
  private final TablesApi tablesApi;

  public SdkTableOperations(ApiClient apiClient) {
    this.tablesApi = new TablesApi(apiClient);
  }

  public TableInfo createTable(CreateTable createTableRequest) throws ApiException {
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
    return tablesApi.getTable(tableFullName);
  }

  @Override
  public void deleteTable(String tableFullName) throws ApiException {
    tablesApi.deleteTable(tableFullName);
  }
}
