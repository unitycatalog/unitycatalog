package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.control.ApiClient;
import io.unitycatalog.control.ApiException;
import io.unitycatalog.control.api.UsersApi;
import io.unitycatalog.control.model.Email;
import io.unitycatalog.control.model.UserResource;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

public class UserCli {
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    UsersApi usersApi = new UsersApi(apiClient);
    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = EMPTY;
    switch (subCommand) {
      case CREATE:
        output = createUser(usersApi, json);
        break;
      case UPDATE:
        output = updateUser(usersApi, json);
        break;
      case LIST:
        output = listUsers(usersApi, json);
        break;
      case GET:
        output = getUser(usersApi, json);
        break;
      case DELETE:
        output = deleteUser(usersApi, json);
        break;
      default:
        printEntityHelp(USER);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String createUser(UsersApi usersApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    List<Email> emails =
        CliUtils.parseToList(json.getString(CliParams.EMAILS.getServerParam()), "\\,").stream()
            .map(e -> new Email().value(e))
            .collect(Collectors.toList());
    if (!emails.isEmpty()) {
      emails.get(0).setPrimary(true);
    }
    String externalId = null;
    if (json.has(CliParams.EXTERNAL_ID.getServerParam())) {
      externalId = json.getString(CliParams.EXTERNAL_ID.getServerParam());
    }
    UserResource userResource =
        new UserResource()
            .displayName(json.getString(CliParams.DISPLAY_NAME.getServerParam()))
            .externalId(externalId)
            .emails(emails);
    return objectWriter.writeValueAsString(usersApi.createUser(userResource));
  }

  private static String updateUser(UsersApi usersApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    List<Email> emails = List.of();
    String displayName = null;
    String externalId = null;

    if (json.has(CliParams.EMAILS.getServerParam())) {
      emails =
          CliUtils.parseToList(json.getString(CliParams.EMAILS.getServerParam()), "\\,").stream()
              .map(e -> new Email().value(e))
              .collect(Collectors.toList());
      if (!emails.isEmpty()) {
        emails.get(0).setPrimary(true);
      }
    }

    if (json.has(CliParams.DISPLAY_NAME.getServerParam())) {
      displayName = json.getString(CliParams.DISPLAY_NAME.getServerParam());
    }

    if (json.has(CliParams.EXTERNAL_ID.getServerParam())) {
      externalId = json.getString(CliParams.EXTERNAL_ID.getServerParam());
    }

    String id = json.getString(CliParams.ID.getServerParam());
    UserResource userResource =
        new UserResource().id(id).displayName(displayName).externalId(externalId).emails(emails);
    return objectWriter.writeValueAsString(usersApi.updateUser(id, userResource));
  }

  private static String listUsers(UsersApi usersApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String filter = null;
    int startIndex = 1;
    int count = 100;
    if (json.has(CliParams.FILTER.getServerParam())) {
      filter = json.getString(CliParams.FILTER.getServerParam());
    }
    if (json.has(CliParams.START_INDEX.getServerParam())) {
      startIndex = json.getInt(CliParams.START_INDEX.getServerParam());
    }
    if (json.has(CliParams.COUNT.getServerParam())) {
      count = json.getInt(CliParams.COUNT.getServerParam());
    }
    return objectWriter.writeValueAsString(usersApi.listUsers(filter, startIndex, count));
  }

  private static String getUser(UsersApi usersApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String id = json.getString(CliParams.ID.getServerParam());
    return objectWriter.writeValueAsString(usersApi.getUser(id));
  }

  private static String deleteUser(UsersApi usersApi, JSONObject json) throws ApiException {
    String id = json.getString(CliParams.ID.getServerParam());
    usersApi.deleteUser(id);
    return EMPTY;
  }
}
