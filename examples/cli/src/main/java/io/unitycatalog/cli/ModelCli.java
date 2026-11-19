package io.unitycatalog.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.cli.utils.Pagination;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.ListRegisteredModelsResponse;
import io.unitycatalog.client.model.UpdateRegisteredModel;
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
    String catalogName = json.optString(CliParams.CATALOG_NAME.getServerParam(), "");
    String schemaName = json.optString(CliParams.SCHEMA_NAME.getServerParam(), "");
    return objectWriter.writeValueAsString(
        Pagination.listAllPages(
            json,
            (maxResults, pageToken) ->
                registeredModelsApi.listRegisteredModels(
                    catalogName, schemaName, maxResults, pageToken, /* includeBrowse= */ false),
            ListRegisteredModelsResponse::getRegisteredModels,
            ListRegisteredModelsResponse::getNextPageToken));
  }

  private static String getRegisteredModel(RegisteredModelsApi registeredModelsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String registeredModelFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    return objectWriter.writeValueAsString(
        registeredModelsApi.getRegisteredModel(
            registeredModelFullName, /* includeBrowse= */ false));
  }

  private static String updateRegisteredModel(
      RegisteredModelsApi registeredModelsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String registeredModelFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    json.remove(CliParams.FULL_NAME.getServerParam());
    CliUtils.validateUpdateParameters(json, CliUtils.REGISTERED_MODEL, CliUtils.UPDATE);
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
