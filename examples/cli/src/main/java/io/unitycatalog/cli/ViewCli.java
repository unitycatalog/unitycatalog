package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.EMPTY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.ViewsApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.CreateView;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.ViewInfo;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.json.JSONException;
import org.json.JSONObject;

public class ViewCli {
  private static final ObjectMapper objectMapper = CliUtils.getObjectMapper();
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    ViewsApi viewsApi = new ViewsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    String subCommand = subArgs[1];
    objectWriter = CliUtils.getObjectWriter(cmd);
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = EMPTY;
    switch (subCommand) {
      case CliUtils.CREATE:
        output = createView(viewsApi, json);
        break;
      case CliUtils.LIST:
        output = listViews(viewsApi, json);
        break;
      case CliUtils.GET:
        output = getView(viewsApi, json);
        break;
      case CliUtils.DELETE:
        output = deleteView(viewsApi, json);
        break;
      default:
        CliUtils.printEntityHelp(CliUtils.VIEW);
    }
    CliUtils.postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String createView(ViewsApi apiClient, JSONObject json)
      throws JsonProcessingException, ApiException {
    CliUtils.resolveFullNameToThreeLevelNamespace(json);
    try {
      json.putOnce(CliParams.TABLE_TYPE.getServerParam(), TableType.VIEW.name());
    } catch (JSONException e) {
      // ignore (table type already set)
    }
    // Set fields in json object for seamless deserialization
    List<ColumnInfo> columnInfoList =
        CliUtils.parseColumns(json.getString(CliParams.COLUMNS.getServerParam()));
    CreateView createView =
        new CreateView()
            .name(json.getString(CliParams.NAME.getServerParam()))
            .catalogName(json.getString(CliParams.CATALOG_NAME.getServerParam()))
            .schemaName(json.getString(CliParams.SCHEMA_NAME.getServerParam()))
            .columns(columnInfoList)
            .properties(CliUtils.extractProperties(objectMapper, json))
            .viewType(TableType.VIEW)
            .sqlQuery(json.getString(CliParams.SQL_QUERY.getServerParam()));
    ViewInfo viewInfo = apiClient.createView(createView);
    return objectWriter.writeValueAsString(viewInfo);
  }

  private static String listViews(ViewsApi viewsApi, JSONObject json)
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
        viewsApi
            .listViews(
                json.getString(CliParams.CATALOG_NAME.getServerParam()),
                json.getString(CliParams.SCHEMA_NAME.getServerParam()),
                maxResults,
                pageToken)
            .getViews());
  }

  private static String getView(ViewsApi viewsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String fullName = json.getString(CliParams.FULL_NAME.val());
    return objectWriter.writeValueAsString(viewsApi.getView(fullName));
  }

  private static String deleteView(ViewsApi viewsApi, JSONObject json) throws ApiException {
    viewsApi.deleteView(json.getString(CliParams.FULL_NAME.getServerParam()));
    return EMPTY;
  }
}
