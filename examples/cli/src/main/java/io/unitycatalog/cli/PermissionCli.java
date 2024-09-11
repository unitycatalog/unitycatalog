package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdatePermissions;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

public class PermissionCli {
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    GrantsApi grantsApi = new GrantsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = EMPTY;
    switch (subCommand) {
      case CREATE:
      case DELETE:
        output = updatePermission(grantsApi, json, subCommand);
        break;
      case GET:
        output = getPermission(grantsApi, json);
        break;
      default:
        printEntityHelp(PERMISSION);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String updatePermission(GrantsApi grantsApi, JSONObject json, String subCommand)
      throws JsonProcessingException, ApiException {
    SecurableType securableType =
        SecurableType.fromValue(json.getString(CliParams.SECURABLE_TYPE.getServerParam()));
    String name = json.getString(CliParams.NAME.getServerParam());
    List<Privilege> add =
        subCommand.equals(CREATE)
            ? List.of(Privilege.fromValue(json.getString(CliParams.PRIVILEGE.getServerParam())))
            : List.of();
    List<Privilege> remove =
        subCommand.equals(DELETE)
            ? List.of(Privilege.fromValue(json.getString(CliParams.PRIVILEGE.getServerParam())))
            : List.of();
    PermissionsChange permissionsChange =
        new PermissionsChange()
            .principal(json.getString(CliParams.PRINCIPAL.getServerParam()))
            .add(add)
            .remove(remove);
    UpdatePermissions updatePermissions =
        new UpdatePermissions().changes(List.of(permissionsChange));

    return objectWriter.writeValueAsString(
        grantsApi.update(securableType, name, updatePermissions).getPrivilegeAssignments());
  }

  private static String getPermission(GrantsApi grantsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    SecurableType securableType =
        SecurableType.fromValue(json.getString(CliParams.SECURABLE_TYPE.getServerParam()));
    String name = json.getString(CliParams.NAME.getServerParam());
    String principal = null;
    if (json.has(CliParams.PRINCIPAL.getServerParam())) {
      principal = json.getString(CliParams.PRINCIPAL.getServerParam());
    }
    return objectWriter.writeValueAsString(
        grantsApi.get(securableType, name, principal).getPrivilegeAssignments());
  }
}
