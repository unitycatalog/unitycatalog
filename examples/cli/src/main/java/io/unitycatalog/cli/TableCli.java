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
import io.unitycatalog.cli.utils.FileOperations;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
<<<<<<< HEAD
import io.unitycatalog.client.model.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
=======
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.TemporaryCredentials;
>>>>>>> 72ddfe72 (Add CLI support for MANAGED tables with CLI tests (#1136))
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
        output = createTable(tablesApi, temporaryCredentialsApi, json);
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

  private static String createTable(
      TablesApi tablesApi, TemporaryCredentialsApi temporaryCredentialsApi, JSONObject json)
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
    TemporaryCredentials temporaryCredentials;
    if (createTable.getTableType() == TableType.EXTERNAL) {
      if (!json.has(CliParams.STORAGE_LOCATION.getServerParam())) {
        throw new CliException("Storage location is required for external tables");
      }
      String storageLocation =
          FileOperations.toStandardizedURIString(
              json.getString(CliParams.STORAGE_LOCATION.getServerParam()));
      createTable.setStorageLocation(storageLocation);
      // Currently generateTemporaryPathCredentials doesn't quite work yet due to lack of proper
      // authorization in its implementation. So this step has to be skipped and only local dir can
      // work. For details please check https://github.com/unitycatalog/unitycatalog/issues/1160
      temporaryCredentials = null;
      // temporaryCredentials =
      //     temporaryCredentialsApi.generateTemporaryPathCredentials(
      //         new GenerateTemporaryPathCredential()
      //             .url(storageLocation)
      //             .operation(PathOperation.PATH_CREATE_TABLE));
    } else if (createTable.getTableType() == TableType.MANAGED) {
      // handle delta managed tables
      if (json.has(CliParams.STORAGE_LOCATION.getServerParam())) {
        throw new CliException("Storage location is not allowed for managed tables");
      }
      // Create staging table if format is delta
      if (!DataSourceFormat.DELTA.name().equals(format.toUpperCase())) {
        throw new CliException("Only delta tables are supported for managed tables: " + format);
      }
      CreateStagingTable createStagingTable =
          new CreateStagingTable()
              .catalogName(json.getString(CliParams.CATALOG_NAME.getServerParam()))
              .schemaName(json.getString(CliParams.SCHEMA_NAME.getServerParam()))
              .name(json.getString(CliParams.NAME.getServerParam()));
      StagingTableInfo stagingTableInfo = tablesApi.createStagingTable(createStagingTable);
      String stagingTableId = stagingTableInfo.getId();
      String stagingLocation = stagingTableInfo.getStagingLocation();
      if (stagingTableId == null || stagingLocation == null) {
        throw new CliException("Failed to create staging table");
      }
      createTable.setStorageLocation(stagingLocation);
      temporaryCredentials =
          temporaryCredentialsApi.generateTemporaryTableCredentials(
              new GenerateTemporaryTableCredential()
                  .tableId(stagingTableId)
                  .operation(TableOperation.READ_WRITE));
    } else {
      throw new CliException("Unknown table type: " + createTable.getTableType());
    }

    // try and initialize the directory and initiate delta log at the location
    try {
      DeltaKernelUtils.createDeltaTable(
          createTable.getStorageLocation(), columnInfoList, temporaryCredentials);
    } catch (Exception e) {
      if ((e instanceof TableAlreadyExistsException
              || e.getCause() instanceof TableAlreadyExistsException)
          && createTable.getTableType() == TableType.EXTERNAL) {
        // A common pattern is that many tests would test the failure cases of table creation. But
        // they often leave the delta table on disk after failing to register a table in UC. Then
        // they retry the table creation on the same location. This would allow those tests to
        // continue. This will only happen to external tables as managed tables would have a unique
        // new location allocated by UC.
        // TODO confirm the schema of the existing table matches the schema of the new table
      } else {
        throw new CliException(
            "Failed to create delta table at " + createTable.getStorageLocation(), e);
      }
    }

    TableInfo tableInfo = tablesApi.createTable(createTable);
    return objectWriter.writeValueAsString(tableInfo);
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
    return objectWriter.writeValueAsString(
        tablesApi.getTable(
            fullName,
            /* readStreamingTableAsManaged = */ true,
            /* readMaterializedViewAsManaged = */ true));
  }

  private static String readTable(
      TemporaryCredentialsApi temporaryCredentialsApi, TablesApi tablesApi, JSONObject json)
      throws ApiException {
    String fullTableName = json.getString(CliParams.FULL_NAME.getServerParam());
    TableInfo info =
        tablesApi.getTable(
            fullTableName,
            /* readStreamingTableAsManaged = */ true,
            /* readMaterializedViewAsManaged = */ true);
    if (!DataSourceFormat.DELTA.equals(info.getDataSourceFormat())) {
      throw new CliException("Only delta tables are supported for read operations");
    }
    String tableId = info.getTableId();
    int maxResults = 100;
    if (json.has(CliParams.MAX_RESULTS.getServerParam())) {
      maxResults = json.getInt(CliParams.MAX_RESULTS.getServerParam());
    }
    try {
      TemporaryCredentials temporaryCredentials =
          temporaryCredentialsApi.generateTemporaryTableCredentials(
              new GenerateTemporaryTableCredential()
                  .tableId(tableId)
                  .operation(TableOperation.READ));
      return DeltaKernelUtils.readDeltaTable(
          info.getStorageLocation(), temporaryCredentials, maxResults);
    } catch (Exception e) {
      throw new CliException("Failed to read delta table " + info.getStorageLocation(), e);
    }
  }

  private static String writeTable(
      TemporaryCredentialsApi temporaryCredentialsApi, TablesApi tablesApi, JSONObject json)
      throws ApiException {
    String fullTableName = json.getString(CliParams.FULL_NAME.getServerParam());
    TableInfo info =
        tablesApi.getTable(
            fullTableName,
            /* readStreamingTableAsManaged = */ true,
            /* readMaterializedViewAsManaged = */ true);
    if (!DataSourceFormat.DELTA.equals(info.getDataSourceFormat())) {
      throw new CliException("Only delta tables are supported for write operations");
    }
    String tableId = info.getTableId();
    try {
      TemporaryCredentials temporaryCredentials =
          temporaryCredentialsApi.generateTemporaryTableCredentials(
              new GenerateTemporaryTableCredential()
                  .tableId(tableId)
                  .operation(TableOperation.READ_WRITE));
      DeltaKernelWriteUtils.writeSampleDataToDeltaTable(
          info.getStorageLocation(), info.getColumns(), temporaryCredentials);
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
}
