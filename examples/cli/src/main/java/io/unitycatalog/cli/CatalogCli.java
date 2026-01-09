package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.postProcessAndPrintOutput;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.UpdateCatalog;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

public class CatalogCli {
  private static final ObjectMapper objectMapper = CliUtils.getObjectMapper();
  private static ObjectWriter objectWriter;
  private static final String NAME_PARAM = CliParams.NAME.val();

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    CatalogsApi catalogsApi = new CatalogsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = CliUtils.EMPTY;
    switch (subCommand) {
      case CliUtils.CREATE:
        output = createCatalog(catalogsApi, json);
        break;
      case CliUtils.LIST:
        output = listCatalogs(catalogsApi, json);
        break;
      case CliUtils.GET:
        output = getCatalog(catalogsApi, json);
        break;
      case CliUtils.UPDATE:
        output = updateCatalog(catalogsApi, json);
        break;
      case CliUtils.DELETE:
        output = deleteCatalog(catalogsApi, json);
        break;
      default:
        CliUtils.printEntityHelp(CliUtils.CATALOG);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String createCatalog(CatalogsApi catalogsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    CreateCatalog createCatalog =
        new CreateCatalog()
            .name(json.getString(CliParams.NAME.getServerParam()))
            .comment(json.optString(CliParams.COMMENT.getServerParam(), null))
            .properties(CliUtils.extractProperties(objectMapper, json));
    CatalogInfo catalogInfo = catalogsApi.createCatalog(createCatalog);
    return objectWriter.writeValueAsString(catalogInfo);
  }

  private static String listCatalogs(CatalogsApi catalogsApi, JSONObject json)
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
        catalogsApi.listCatalogs(pageToken, maxResults).getCatalogs());
  }

  private static String getCatalog(CatalogsApi catalogsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String catalogName = json.getString(NAME_PARAM);
    return objectWriter.writeValueAsString(catalogsApi.getCatalog(catalogName));
  }

  private static String updateCatalog(CatalogsApi catalogsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String catalogName = json.getString(NAME_PARAM);
    json.remove(NAME_PARAM);
    if (json.length() == 0) {
      List<CliParams> optionalParams =
          CliUtils.cliOptions.get(CliUtils.CATALOG).get(CliUtils.UPDATE).getOptionalParams();
      String errorMessage = "No parameters to update, please provide one of:";
      for (CliParams param : optionalParams) {
        errorMessage += "\n  --" + param.val();
      }
      throw new CliException(errorMessage);
    }
    UpdateCatalog updateCatalog =
        new UpdateCatalog()
            .newName(json.optString(CliParams.NEW_NAME.getServerParam(), null))
            .comment(json.optString(CliParams.COMMENT.getServerParam(), null))
            .properties(CliUtils.extractProperties(objectMapper, json));
    CatalogInfo catalogInfo = catalogsApi.updateCatalog(catalogName, updateCatalog);
    return objectWriter.writeValueAsString(catalogInfo);
  }

  private static String deleteCatalog(CatalogsApi catalogsApi, JSONObject json)
      throws ApiException {
    String catalogName = json.getString(NAME_PARAM);
    catalogsApi.deleteCatalog(
        catalogName,
        json.has(CliParams.FORCE.getServerParam())
            && Boolean.parseBoolean(json.getString(CliParams.FORCE.getServerParam())));
    return CliUtils.EMPTY_JSON;
  }
}
