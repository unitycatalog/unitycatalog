package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.EMPTY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.delta.kernel.exceptions.TableAlreadyExistsException;
import io.unitycatalog.cli.delta.DeltaKernelUtils;
import io.unitycatalog.cli.delta.DeltaKernelWriteUtils;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.json.JSONException;
import org.json.JSONObject;

public class TableCli {
  private static final ObjectMapper objectMapper = CliUtils.getObjectMapper();
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    TablesApi tablesApi = new TablesApi(apiClient);
    TemporaryCredentialsApi temporaryCredentialsApi = new TemporaryCredentialsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    String subCommand = subArgs[1];
    objectWriter = CliUtils.getObjectWriter(cmd);
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = EMPTY;
    switch (subCommand) {
      case CliUtils.CREATE:
        output = createTable(tablesApi, json);
        break;
      case CliUtils.LIST:
        output = listTables(tablesApi, json);
        break;
      case CliUtils.GET:
        output = getTable(tablesApi, json);
        break;
      case CliUtils.READ:
        output = readTable(temporaryCredentialsApi, tablesApi, json);
        break;
      case CliUtils.WRITE:
        output = writeTable(temporaryCredentialsApi, tablesApi, json);
        break;
      case CliUtils.DELETE:
        output = deleteTable(tablesApi, json);
        break;
      default:
        CliUtils.printEntityHelp(CliUtils.TABLE);
    }
    CliUtils.postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String createTable(TablesApi apiClient, JSONObject json)
      throws JsonProcessingException, ApiException {
    CliUtils.resolveFullNameToThreeLevelNamespace(json);
    try {
      json.putOnce(CliParams.TABLE_TYPE.getServerParam(), TableType.EXTERNAL.name());
    } catch (JSONException e) {
      // ignore (table type already set)
    }
    try {
      json.putOnce(CliParams.DATA_SOURCE_FORMAT.getServerParam(), DataSourceFormat.DELTA.name());
    } catch (JSONException e) {
      // ignore (data source format already set)
    }
    String format = json.getString(CliParams.DATA_SOURCE_FORMAT.getServerParam());
    // Set fields in json object for seamless deserialization
    List<ColumnInfo> columnInfoList =
        CliUtils.parseColumns(json.getString(CliParams.COLUMNS.getServerParam()));
    CreateTable createTable =
        new CreateTable()
            .name(json.getString(CliParams.NAME.getServerParam()))
            .catalogName(json.getString(CliParams.CATALOG_NAME.getServerParam()))
            .schemaName(json.getString(CliParams.SCHEMA_NAME.getServerParam()))
            .columns(columnInfoList)
            .properties(CliUtils.extractProperties(objectMapper, json))
            .tableType(
                TableType.valueOf(
                    json.getString(CliParams.TABLE_TYPE.getServerParam()).toUpperCase()))
            .dataSourceFormat(DataSourceFormat.valueOf(format.toUpperCase()));
    if (createTable.getTableType() == TableType.EXTERNAL) {
      createTable.storageLocation(json.getString(CliParams.STORAGE_LOCATION.getServerParam()));
      handleTableStorageLocation(createTable.getStorageLocation(), columnInfoList);
    }
    TableInfo tableInfo = apiClient.createTable(createTable);
    return objectWriter.writeValueAsString(tableInfo);
  }

  private static Path getLocalPath(String path) {
    if (path.startsWith("file:")) {
      return Paths.get(URI.create(path));
    } else {
      return Paths.get(path);
    }
  }

  private static void handleTableStorageLocation(
      String storageLocation, List<ColumnInfo> columnInfos) {
    if (!(storageLocation.startsWith("s3://")
        || storageLocation.startsWith("file:/")
        || storageLocation.startsWith("/"))) {
      throw new CliException(
          "Storage location must start with s3:// or file:/ or be an absolute local filesystem path.");
    }
    if (!storageLocation.startsWith("s3://")) {
      // local filesystem path
      Path path = getLocalPath(storageLocation);
      // try and initialize the directory and initiate delta log at the location
      try {
        DeltaKernelUtils.createDeltaTable(path.toUri().toString(), columnInfos, null);
      } catch (Exception e) {
        if (e.getCause() instanceof TableAlreadyExistsException) {
          // TODO confirm the schema of the existing table matches the schema of the new table
        } else {
          throw new CliException("Failed to create delta table at " + path, e);
        }
      }
    }
  }

  private static String listTables(TablesApi tablesApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    int maxResults = 100;
    if (json.has(CliParams.MAX_RESULTS.getServerParam())) {
      maxResults = json.getInt(CliParams.MAX_RESULTS.getServerParam());
    }
    String pageToken = null;
    if (json.has(CliParams.PAGE_TOKEN.getServerParam())) {
      pageToken = json.getString(CliParams.PAGE_TOKEN.getServerParam());
    }
    return objectWriter.writeValueAsString(
        tablesApi
            .listTables(
                json.getString(CliParams.CATALOG_NAME.getServerParam()),
                json.getString(CliParams.SCHEMA_NAME.getServerParam()),
                maxResults,
                pageToken)
            .getTables());
  }

  private static String getTable(TablesApi tablesApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String fullName = json.getString(CliParams.FULL_NAME.val());
    return objectWriter.writeValueAsString(tablesApi.getTable(fullName));
  }

  private static String readTable(
      TemporaryCredentialsApi temporaryCredentialsApi, TablesApi tablesApi, JSONObject json)
      throws ApiException {
    String fullTableName = json.getString(CliParams.FULL_NAME.getServerParam());
    TableInfo info = tablesApi.getTable(fullTableName);
    if (!DataSourceFormat.DELTA.equals(info.getDataSourceFormat())) {
      throw new CliException("Only delta tables are supported for read operations");
    }
    String tableId = info.getTableId();
    int maxResults = 100;
    if (json.has(CliParams.MAX_RESULTS.getServerParam())) {
      maxResults = json.getInt(CliParams.MAX_RESULTS.getServerParam());
    }
    try {
      return DeltaKernelUtils.readDeltaTable(
          info.getStorageLocation(),
          getTemporaryTableCredentials(temporaryCredentialsApi, tableId, TableOperation.READ),
          maxResults);
    } catch (Exception e) {
      throw new CliException("Failed to read delta table " + info.getStorageLocation(), e);
    }
  }

  private static String writeTable(
      TemporaryCredentialsApi temporaryCredentialsApi, TablesApi tablesApi, JSONObject json)
      throws ApiException {
    String fullTableName = json.getString(CliParams.FULL_NAME.getServerParam());
    TableInfo info = tablesApi.getTable(fullTableName);
    if (!DataSourceFormat.DELTA.equals(info.getDataSourceFormat())) {
      throw new CliException("Only delta tables are supported for write operations");
    }
    String tableId = info.getTableId();
    try {
      DeltaKernelWriteUtils.writeSampleDataToDeltaTable(
          info.getStorageLocation(),
          info.getColumns(),
          getTemporaryTableCredentials(
              temporaryCredentialsApi, tableId, TableOperation.READ_WRITE));
    } catch (Exception e) {
      throw new CliException(
          "Failed to write sample data to delta table " + info.getStorageLocation(), e);
    }
    return EMPTY;
  }

  private static String deleteTable(TablesApi tablesApi, JSONObject json) throws ApiException {
    tablesApi.deleteTable(json.getString(CliParams.FULL_NAME.getServerParam()));
    return EMPTY;
  }

  public static AwsCredentials getTemporaryTableCredentials(
      TemporaryCredentialsApi apiClient, String tableId, TableOperation operation)
      throws ApiException {
    TemporaryCredentials temporaryTableCredentials =
        apiClient.generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential().tableId(tableId).operation(operation));
    return temporaryTableCredentials.getAwsTempCredentials();
  }
}
