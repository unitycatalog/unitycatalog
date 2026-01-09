package io.unitycatalog.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.UpdateRegisteredModel;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

public class ModelCli {
  private static final ObjectMapper objectMapper = CliUtils.getObjectMapper();
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    RegisteredModelsApi registeredModelsApi = new RegisteredModelsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    String subCommand = subArgs[1];
    objectWriter = CliUtils.getObjectWriter(cmd);
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = CliUtils.EMPTY;
    switch (subCommand) {
      case CliUtils.CREATE:
        output = createRegisteredModel(registeredModelsApi, json);
        break;
      case CliUtils.LIST:
        output = listRegisteredModels(registeredModelsApi, json);
        break;
      case CliUtils.GET:
        output = getRegisteredModel(registeredModelsApi, json);
        break;
      case CliUtils.UPDATE:
        output = updateRegisteredModel(registeredModelsApi, json);
        break;
      case CliUtils.DELETE:
        output = deleteRegisteredModel(registeredModelsApi, json);
        break;
      default:
        CliUtils.printEntityHelp(CliUtils.REGISTERED_MODEL);
    }
    CliUtils.postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String createRegisteredModel(
      RegisteredModelsApi registeredModelsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    CreateRegisteredModel createRegisteredModel;
    createRegisteredModel = objectMapper.readValue(json.toString(), CreateRegisteredModel.class);
    return objectWriter.writeValueAsString(
        registeredModelsApi.createRegisteredModel(createRegisteredModel));
  }

  private static String listRegisteredModels(
      RegisteredModelsApi registeredModelsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String catalogName = "";
    String schemaName = "";
    if (json.has(CliParams.CATALOG_NAME.getServerParam())) {
      catalogName = json.getString(CliParams.CATALOG_NAME.getServerParam());
    }
    if (json.has(CliParams.SCHEMA_NAME.getServerParam())) {
      schemaName = json.getString(CliParams.SCHEMA_NAME.getServerParam());
    }
    int maxResults = 100;
    if (json.has(CliParams.MAX_RESULTS.getServerParam())) {
      maxResults = json.getInt(CliParams.MAX_RESULTS.getServerParam());
    }
    String pageToken = null;
    if (json.has(CliParams.PAGE_TOKEN.getServerParam())) {
      pageToken = json.getString(CliParams.PAGE_TOKEN.getServerParam());
    }
    return objectWriter.writeValueAsString(
        registeredModelsApi
            .listRegisteredModels(catalogName, schemaName, maxResults, pageToken)
            .getRegisteredModels());
  }

  private static String getRegisteredModel(RegisteredModelsApi registeredModelsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String registeredModelFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    return objectWriter.writeValueAsString(
        registeredModelsApi.getRegisteredModel(registeredModelFullName));
  }

  private static String updateRegisteredModel(
      RegisteredModelsApi registeredModelsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String registeredModelFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    json.remove(CliParams.FULL_NAME.getServerParam());
    if (json.length() == 0) {
      List<CliParams> optionalParams =
          CliUtils.cliOptions
              .get(CliUtils.REGISTERED_MODEL)
              .get(CliUtils.UPDATE)
              .getOptionalParams();
      String errorMessage = "No parameters to update, please provide one of:";
      for (CliParams param : optionalParams) {
        errorMessage += "\n  --" + param.val();
      }
      throw new RuntimeException(errorMessage);
    }
    UpdateRegisteredModel updateRegisteredModel =
        objectMapper.readValue(json.toString(), UpdateRegisteredModel.class);
    return objectWriter.writeValueAsString(
        registeredModelsApi.updateRegisteredModel(registeredModelFullName, updateRegisteredModel));
  }

  private static String deleteRegisteredModel(
      RegisteredModelsApi registeredModelsApi, JSONObject json) throws ApiException {
    String registeredModelFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    registeredModelsApi.deleteRegisteredModel(
        registeredModelFullName,
        json.has(CliParams.FORCE.getServerParam())
            && Boolean.parseBoolean(json.getString(CliParams.FORCE.getServerParam())));
    return CliUtils.EMPTY;
  }
}
