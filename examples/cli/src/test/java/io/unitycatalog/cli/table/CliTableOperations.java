package io.unitycatalog.cli.table;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.table.TableOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliTableOperations implements TableOperations {
  private final ServerConfig config;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public CliTableOperations(ServerConfig config) {
    this.config = config;
  }

  @Override
  public TableInfo createTable(CreateTable createTableRequest) {
    StringBuilder columns = new StringBuilder();
    for (ColumnInfo column : createTableRequest.getColumns()) {
      columns.append(column.getName()).append(" ").append(column.getTypeName().name()).append(",");
    }
    columns.deleteCharAt(columns.length() - 1);

    List<String> argsList = new ArrayList<>();
    argsList.addAll(
        List.of(
            "table",
            "create",
            "--full_name",
            createTableRequest.getCatalogName()
                + "."
                + createTableRequest.getSchemaName()
                + "."
                + createTableRequest.getName(),
            "--columns",
            columns.toString()));
    if (createTableRequest.getDataSourceFormat() != null) {
      argsList.add("--format");
      argsList.add(createTableRequest.getDataSourceFormat().name());
    }
    if (createTableRequest.getStorageLocation() != null) {
      argsList.add("--storage_location");
      argsList.add(createTableRequest.getStorageLocation());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode tableJson = executeCLICommand(args);
    return objectMapper.convertValue(tableJson, TableInfo.class);
  }

  @Override
  public List<TableInfo> listTables(
      String catalogName, String schemaName, Optional<String> pageToken) {
    List<String> argsList =
        new ArrayList<>(List.of("table", "list", "--catalog", catalogName, "--schema", schemaName));
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode tableListJson = executeCLICommand(args);
    return objectMapper.convertValue(tableListJson, new TypeReference<List<TableInfo>>() {});
  }

  @Override
  public TableInfo getTable(String tableFullName) {
    String[] args =
        addServerAndAuthParams(List.of("table", "get", "--full_name", tableFullName), config);
    JsonNode tableJson = executeCLICommand(args);
    return objectMapper.convertValue(tableJson, TableInfo.class);
  }

  @Override
  public void deleteTable(String tableFullName) {
    String[] args =
        addServerAndAuthParams(List.of("table", "delete", "--full_name", tableFullName), config);
    executeCLICommand(args);
  }
}
