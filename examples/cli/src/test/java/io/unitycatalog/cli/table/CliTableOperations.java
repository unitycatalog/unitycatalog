package io.unitycatalog.cli.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.cli.BaseCliOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.table.TableOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliTableOperations extends BaseCliOperations implements TableOperations {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public CliTableOperations(ServerConfig config) {
    super("table", config);
  }

  @Override
  public TableInfo createTable(CreateTable createTableRequest) throws ApiException {
    StringBuilder columns = new StringBuilder();
    for (ColumnInfo column : createTableRequest.getColumns()) {
      columns.append(column.getName()).append(" ").append(column.getTypeName().name()).append(",");
    }
    columns.deleteCharAt(columns.length() - 1);

    List<String> argsList =
        new ArrayList<>(
            List.of(
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
    if (createTableRequest.getTableType() != null) {
      argsList.add("--table_type");
      argsList.add(createTableRequest.getTableType().name());
    }
    if (createTableRequest.getViewDefinition() != null) {
      argsList.add("--view_definition");
      argsList.add(createTableRequest.getViewDefinition());
    }
    if (createTableRequest.getViewDependencies() != null) {
      argsList.add("--view_dependencies");
      try {
        argsList.add(
            OBJECT_MAPPER.writeValueAsString(
                createTableRequest.getViewDependencies().getDependencies()));
      } catch (JsonProcessingException e) {
        throw new ApiException(e);
      }
    }
    return execute(TableInfo.class, "create", argsList);
  }

  @Override
  public List<TableInfo> listTables(
      String catalogName, String schemaName, Optional<String> pageToken) throws ApiException {
    List<String> argsList =
        new ArrayList<>(List.of("--catalog", catalogName, "--schema", schemaName));
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    return execute(new TypeReference<>() {}, "list", argsList);
  }

  @Override
  public TableInfo getTable(String tableFullName) throws ApiException {
    return execute(TableInfo.class, "get", List.of("--full_name", tableFullName));
  }

  @Override
  public void deleteTable(String tableFullName) throws ApiException {
    execute(Void.class, "delete", List.of("--full_name", tableFullName));
  }
}
