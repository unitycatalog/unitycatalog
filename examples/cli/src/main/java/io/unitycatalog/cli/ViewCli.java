package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.EMPTY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.ViewsApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.CreateView;
import io.unitycatalog.client.model.ViewInfo;
import io.unitycatalog.client.model.ViewRepresentation;
import java.util.List;
import org.apache.commons.cli.CommandLine;
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

  private static String createView(ViewsApi viewsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    CliUtils.resolveFullNameToThreeLevelNamespace(json);

    List<ColumnInfo> columnInfoList =
        CliUtils.parseColumns(json.getString(CliParams.COLUMNS.getServerParam()));

    // Parse view definition from JSON string
    List<ViewRepresentation> viewDefinition;
    try {
      String viewDefJson = json.getString(CliParams.VIEW_DEFINITION.getServerParam());
      viewDefinition =
          objectMapper.readValue(viewDefJson, new TypeReference<List<ViewRepresentation>>() {});
    } catch (Exception e) {
      throw new CliException(
          "Failed to parse view_definition. Expected JSON array format: "
              + "[{\"dialect\":\"spark\",\"sql\":\"SELECT * FROM table\"}]",
          e);
    }

    CreateView createView =
        new CreateView()
            .name(json.getString(CliParams.NAME.getServerParam()))
            .catalogName(json.getString(CliParams.CATALOG_NAME.getServerParam()))
            .schemaName(json.getString(CliParams.SCHEMA_NAME.getServerParam()))
            .columns(columnInfoList)
            .viewDefinition(viewDefinition)
            .properties(CliUtils.extractProperties(objectMapper, json));

    if (json.has(CliParams.COMMENT.getServerParam())) {
      createView.setComment(json.getString(CliParams.COMMENT.getServerParam()));
    }

    ViewInfo viewInfo = viewsApi.createView(createView);
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
