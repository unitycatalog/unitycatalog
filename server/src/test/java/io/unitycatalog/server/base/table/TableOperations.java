package io.unitycatalog.server.base.table;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.TableInfo;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface TableOperations {
  TableInfo createTable(CreateTable createTableRequest) throws IOException, ApiException;

  List<TableInfo> listTables(String catalogName, String schemaName, Optional<String> pageToken)
      throws ApiException;

  TableInfo getTable(String tableFullName) throws ApiException;
  // TableInfo updateTable(UpdateTableRequestContent updateTableRequest) throws IOException;
  void deleteTable(String tableFullName) throws ApiException;
}
