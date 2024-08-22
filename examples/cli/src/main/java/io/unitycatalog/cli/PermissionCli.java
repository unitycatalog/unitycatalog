package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
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
      case DELETE:
        output = updatePermission(permissionsApi, json, subCommand);
        break;
      case GET:
        output = getPermission(permissionsApi, json);
        break;
      default:
        printEntityHelp(PERMISSION);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String updatePermission(
      PermissionsApi permissionsApi, JSONObject json, String subCommand)
      throws JsonProcessingException, ApiException {
    ResourceType resourceType =
        ResourceType.fromValue(json.getString(CliParams.RESOURCE_TYPE.getServerParam()));
    String name = json.getString(CliParams.NAME.getServerParam());
    List<Privilege> add =
        subCommand.equals(CREATE)
            ? List.of(Privilege.fromValue(json.getString(CliParams.PRIVILEGE.getServerParam())))
            : List.of();
    List<Privilege> remove =
        subCommand.equals(DELETE)
            ? List.of(Privilege.fromValue(json.getString(CliParams.PRIVILEGE.getServerParam())))
            : List.of();
    UpdateAuthorizationChange updateAuthorizationChange =
        new UpdateAuthorizationChange()
            .principal(json.getString(CliParams.PRINCIPAL.getServerParam()))
            .add(add)
            .remove(remove);
    UpdateAuthorizationRequest updateAuthorizationRequest =
        new UpdateAuthorizationRequest().changes(List.of(updateAuthorizationChange));

    return objectWriter.writeValueAsString(
        permissionsApi
            .updatePermission(resourceType, name, updateAuthorizationRequest)
            .getPrivilegeAssignments());
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
        permissionsApi.getPermission(resourceType, name, principal).getPrivilegeAssignments());
  }
}
