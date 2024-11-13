package io.unitycatalog.server.base.table;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.UpdateTable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface TableOperations {
  TableInfo createTable(CreateTable createTableRequest) throws IOException, ApiException;

  List<TableInfo> listTables(String catalogName, String schemaName, Optional<String> pageToken)
      throws ApiException;

  TableInfo getTable(String tableFullName) throws ApiException;

  void deleteTable(String tableFullName) throws ApiException;

  TableInfo updateTable(String tableFullName, UpdateTable updateTableRequest) throws ApiException;
}
