package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.PermissionsApi;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.ResourceType;
import io.unitycatalog.client.model.UpdateAuthorizationChange;
import io.unitycatalog.client.model.UpdateAuthorizationRequest;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

public class PermissionCli {
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    PermissionsApi permissionsApi = new PermissionsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = EMPTY;
    switch (subCommand) {
      case CREATE:
      case UPDATE:
        output = updatePermission(permissionsApi, json);
        break;
      case GET:
        output = getPermission(permissionsApi, json);
        break;
      default:
        printEntityHelp(PERMISSION);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String updatePermission(PermissionsApi permissionsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    ResourceType resourceType =
        ResourceType.fromValue(json.getString(CliParams.RESOURCE_TYPE.getServerParam()));
    String name = json.getString(CliParams.NAME.getServerParam());
    List<Privilege> add = List.of();
    if (json.has(CliParams.ADD.getServerParam())) {
      add =
          CliUtils.parseToList(json.getString(CliParams.ADD.getServerParam()), "\\,").stream()
              .map(Privilege::fromValue)
              .collect(Collectors.toList());
    }

    List<Privilege> remove = List.of();
    if (json.has(CliParams.REMOVE.getServerParam())) {
      remove =
          CliUtils.parseToList(json.getString(CliParams.REMOVE.getServerParam()), "\\,").stream()
              .map(Privilege::fromValue)
              .collect(Collectors.toList());
    }
    UpdateAuthorizationChange updateAuthorizationChange =
        new UpdateAuthorizationChange()
            .principal(json.getString(CliParams.PRINCIPAL.getServerParam()))
            .add(add)
            .remove(remove);
    UpdateAuthorizationRequest updateAuthorizationRequest =
        new UpdateAuthorizationRequest().changes(List.of(updateAuthorizationChange));

    return objectWriter.writeValueAsString(
        permissionsApi.updatePermission(resourceType, name, updateAuthorizationRequest));
  }

  private static String getPermission(PermissionsApi permissionsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    ResourceType resourceType =
        ResourceType.fromValue(json.getString(CliParams.RESOURCE_TYPE.getServerParam()));
    String name = json.getString(CliParams.NAME.getServerParam());
    String principal = null;
    if (json.has(CliParams.PRINCIPAL.getServerParam())) {
      principal = json.getString(CliParams.PRINCIPAL.getServerParam());
    }
    return objectWriter.writeValueAsString(
        permissionsApi.getPermission(resourceType, name, principal));
  }
}
