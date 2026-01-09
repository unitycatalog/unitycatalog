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
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.ListExternalLocationsResponse;
import io.unitycatalog.client.model.UpdateExternalLocation;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

public class ExternalLocationCli {
  private static final ObjectMapper objectMapper = CliUtils.getObjectMapper();
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    ExternalLocationsApi externalLocationsApi = new ExternalLocationsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = CliUtils.EMPTY;
    switch (subCommand) {
      case CliUtils.CREATE:
        output = createExternalLocation(externalLocationsApi, json);
        break;
      case CliUtils.LIST:
        output = listExternalLocations(externalLocationsApi, json);
        break;
      case CliUtils.GET:
        output = getExternalLocation(externalLocationsApi, json);
        break;
      case CliUtils.UPDATE:
        output = updateExternalLocation(externalLocationsApi, json);
        break;
      case CliUtils.DELETE:
        output = deleteExternalLocation(externalLocationsApi, json);
        break;
      default:
        CliUtils.printEntityHelp(CliUtils.EXTERNAL_LOCATION);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String createExternalLocation(
      ExternalLocationsApi externalLocationsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    CreateExternalLocation createExternalLocation =
        objectMapper.readValue(json.toString(), CreateExternalLocation.class);
    ExternalLocationInfo externalLocationInfo =
        externalLocationsApi.createExternalLocation(createExternalLocation);
    return objectWriter.writeValueAsString(externalLocationInfo);
  }

  private static String listExternalLocations(
      ExternalLocationsApi externalLocationsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    int maxResults = json.optInt(CliParams.MAX_RESULTS.getServerParam(), 100);
    String pageToken = json.optString(CliParams.PAGE_TOKEN.getServerParam(), null);

    ListExternalLocationsResponse listExternalLocationsResponse =
        externalLocationsApi.listExternalLocations(maxResults, pageToken);
    List<ExternalLocationInfo> externalLocations =
        listExternalLocationsResponse.getExternalLocations();
    return objectWriter.writeValueAsString(externalLocations);
  }

  private static String getExternalLocation(
      ExternalLocationsApi externalLocationsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String name = json.getString(CliParams.NAME.getServerParam());
    ExternalLocationInfo externalLocationInfo = externalLocationsApi.getExternalLocation(name);
    return objectWriter.writeValueAsString(externalLocationInfo);
  }

  private static String updateExternalLocation(
      ExternalLocationsApi externalLocationsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String name = json.getString(CliParams.NAME.getServerParam());
    json.remove(CliParams.NAME.getServerParam()); // Remove name from JSON as it's a path parameter
    if (json.isEmpty()) {
      List<CliParams> optionalParams =
          CliUtils.cliOptions
              .get(CliUtils.EXTERNAL_LOCATION)
              .get(CliUtils.UPDATE)
              .getOptionalParams();
      String errorMessage = "No parameters to update, please provide one of:";
      for (CliParams param : optionalParams) {
        errorMessage += "\n  --" + param.val();
      }
      throw new CliException(errorMessage);
    }
    UpdateExternalLocation updateExternalLocation =
        objectMapper.readValue(json.toString(), UpdateExternalLocation.class);
    ExternalLocationInfo externalLocationInfo =
        externalLocationsApi.updateExternalLocation(name, updateExternalLocation);
    return objectWriter.writeValueAsString(externalLocationInfo);
  }

  private static String deleteExternalLocation(
      ExternalLocationsApi externalLocationsApi, JSONObject json) throws ApiException {
    String name = json.getString(CliParams.NAME.getServerParam());
    boolean force = json.optBoolean(CliParams.FORCE.getServerParam(), false);
    externalLocationsApi.deleteExternalLocation(name, force);
    return CliUtils.EMPTY_JSON;
  }
}
