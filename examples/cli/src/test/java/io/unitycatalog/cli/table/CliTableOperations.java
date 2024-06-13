package io.unitycatalog.cli.table;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.table.TableOperations;

import java.io.IOException;
import java.util.List;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

public class CliTableOperations implements TableOperations  {
    private final ServerConfig config;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CliTableOperations(ServerConfig config) {
        this.config = config;
    }

    @Override
    public TableInfo createTable(CreateTable createTableRequest) throws IOException {

        StringBuilder columns = new StringBuilder();
        for (ColumnInfo column : createTableRequest.getColumns()) {
            columns.append(column.getName()).append(" ").append(column.getTypeName().name()).append(",");
        }
        columns.deleteCharAt(columns.length() - 1);

        String [] args = addServerAndAuthParams(List.of(
                "table", "create" ,
                "--full_name", createTableRequest.getCatalogName() + "." + createTableRequest.getSchemaName() + "." + createTableRequest.getName(),
                "--columns", columns.toString(),
                "--format", createTableRequest.getDataSourceFormat() != null ? createTableRequest.getDataSourceFormat().name() : null,
                "--storage_location", createTableRequest.getStorageLocation() != null ? createTableRequest.getStorageLocation() : null
        ), config);
        JsonNode tableJson = executeCLICommand(args);
        return objectMapper.convertValue(tableJson, TableInfo.class);
    }

    @Override
    public List<TableInfo> listTables(String catalogName, String schemaName) {
        String [] args = addServerAndAuthParams(List.of("table", "list", "--catalog", catalogName, "--schema", schemaName), config);
        JsonNode tableListJson = executeCLICommand(args);
        return objectMapper.convertValue(tableListJson, new TypeReference<List<TableInfo>>(){});
    }

    @Override
    public TableInfo getTable(String tableFullName) {
        String [] args = addServerAndAuthParams(List.of("table", "get", "--full_name", tableFullName), config);
        JsonNode tableJson = executeCLICommand(args);
        return objectMapper.convertValue(tableJson, TableInfo.class);
    }

    @Override
    public void deleteTable(String tableFullName) {
        String [] args = addServerAndAuthParams(List.of("table", "delete", "--full_name", tableFullName), config);
        executeCLICommand(args);
    }
}
