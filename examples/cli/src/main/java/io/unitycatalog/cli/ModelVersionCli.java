package io.unitycatalog.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.ModelVersionsApi;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.FinalizeModelVersion;
import io.unitycatalog.client.model.UpdateModelVersion;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

public class ModelVersionCli {
  private static final ObjectMapper objectMapper = CliUtils.getObjectMapper();
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    ModelVersionsApi modelVersionsApi = new ModelVersionsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    String subCommand = subArgs[1];
    objectWriter = CliUtils.getObjectWriter(cmd);
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = CliUtils.EMPTY;
    switch (subCommand) {
      case CliUtils.CREATE:
        output = createModelVersion(modelVersionsApi, json);
        break;
      case CliUtils.LIST:
        output = listModelVersions(modelVersionsApi, json);
        break;
      case CliUtils.GET:
        output = getModelVersion(modelVersionsApi, json);
        break;
      case CliUtils.UPDATE:
        output = updateModelVersion(modelVersionsApi, json);
        break;
      case CliUtils.DELETE:
        output = deleteModelVersion(modelVersionsApi, json);
        break;
      case CliUtils.FINALIZE:
        output = finalizeModelVersion(modelVersionsApi, json);
        break;
      default:
        CliUtils.printEntityHelp(CliUtils.REGISTERED_MODEL);
    }
    CliUtils.postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String createModelVersion(ModelVersionsApi modelVersionsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    CreateModelVersion createModelVersion;
    // Map NAME to model_name for the CreateModelVersion request
    String modelName = json.getString(CliParams.NAME.val());
    json.put(CliParams.MODEL_NAME.val(), modelName);
    json.remove(CliParams.NAME.val());
    createModelVersion = objectMapper.readValue(json.toString(), CreateModelVersion.class);
    return objectWriter.writeValueAsString(modelVersionsApi.createModelVersion(createModelVersion));
  }

  private static String listModelVersions(ModelVersionsApi modelVersionsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String fullName = json.getString(CliParams.FULL_NAME.getServerParam());
    int maxResults = 100;
    if (json.has(CliParams.MAX_RESULTS.getServerParam())) {
      maxResults = json.getInt(CliParams.MAX_RESULTS.getServerParam());
    }
    String pageToken = null;
    if (json.has(CliParams.PAGE_TOKEN.getServerParam())) {
      pageToken = json.getString(CliParams.PAGE_TOKEN.getServerParam());
    }
    return objectWriter.writeValueAsString(
        modelVersionsApi.listModelVersions(fullName, maxResults, pageToken).getModelVersions());
  }

  private static String getModelVersion(ModelVersionsApi modelVersionsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String registeredModelFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    Long version = json.getLong(CliParams.VERSION.getServerParam());
    return objectWriter.writeValueAsString(
        modelVersionsApi.getModelVersion(registeredModelFullName, version));
  }

  private static String updateModelVersion(ModelVersionsApi modelVersionsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String registeredModelFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    json.remove(CliParams.FULL_NAME.getServerParam());
    Long version = json.getLong(CliParams.VERSION.getServerParam());
    json.remove(CliParams.VERSION.getServerParam());
    UpdateModelVersion updateModelVersion =
        objectMapper.readValue(json.toString(), UpdateModelVersion.class);
    return objectWriter.writeValueAsString(
        modelVersionsApi.updateModelVersion(registeredModelFullName, version, updateModelVersion));
  }

  private static String deleteModelVersion(ModelVersionsApi modelVersionsApi, JSONObject json)
      throws ApiException {
    String registeredModelFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    Long version = json.getLong(CliParams.VERSION.getServerParam());
    modelVersionsApi.deleteModelVersion(registeredModelFullName, version);
    return CliUtils.EMPTY;
  }

  private static String finalizeModelVersion(ModelVersionsApi modelVersionsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String registeredModelFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    Long version = json.getLong(CliParams.VERSION.getServerParam());
    FinalizeModelVersion finalizeModelVersion =
        objectMapper.readValue(json.toString(), FinalizeModelVersion.class);
    finalizeModelVersion.setFullName(registeredModelFullName);
    finalizeModelVersion.setVersion(version);
    return objectWriter.writeValueAsString(
        modelVersionsApi.finalizeModelVersion(
            registeredModelFullName, version, finalizeModelVersion));
  }
}
