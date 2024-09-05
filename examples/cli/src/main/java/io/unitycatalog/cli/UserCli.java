package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.*;
import static io.unitycatalog.control.model.User.StateEnum.DISABLED;
import static io.unitycatalog.control.model.User.StateEnum.ENABLED;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.control.ApiClient;
import io.unitycatalog.control.ApiException;
import io.unitycatalog.control.api.UsersApi;
import io.unitycatalog.control.model.Email;
import io.unitycatalog.control.model.User;
import io.unitycatalog.control.model.UserResource;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
        List.of(new Email().value(json.getString(CliParams.EMAIL.getServerParam())).primary(true));
    String externalId = null;
    if (json.has(CliParams.EXTERNAL_ID.getServerParam())) {
      externalId = json.getString(CliParams.EXTERNAL_ID.getServerParam());
    }
    UserResource userResource =
        new UserResource()
            .displayName(json.getString(CliParams.NAME.getServerParam()))
            .externalId(externalId)
            .emails(emails);
    UserResource user = usersApi.createUser(userResource);
    return objectWriter.writeValueAsString(fromUserResource(user));
  }

  private static String updateUser(UsersApi usersApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    List<Email> emails = new ArrayList<>();
    String displayName = null;
    String externalId = null;

    if (json.has(CliParams.EMAIL.getServerParam())) {
      emails.add(new Email().value(json.getString(CliParams.EMAIL.getServerParam())).primary(true));
    }

    if (json.has(CliParams.NAME.getServerParam())) {
      displayName = json.getString(CliParams.NAME.getServerParam());
    }

    if (json.has(CliParams.EXTERNAL_ID.getServerParam())) {
      externalId = json.getString(CliParams.EXTERNAL_ID.getServerParam());
    }

    String id = json.getString(CliParams.ID.getServerParam());
    UserResource userResource =
        new UserResource().id(id).displayName(displayName).externalId(externalId).emails(emails);
    UserResource user = usersApi.updateUser(id, userResource);
    return objectWriter.writeValueAsString(fromUserResource(user));
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

    List<User> users =
        usersApi.listUsers(filter, startIndex, count).getResources().stream()
            .map(UserCli::fromUserResource)
            .collect(Collectors.toList());
    return objectWriter.writeValueAsString(users);
  }

  private static String getUser(UsersApi usersApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String id = json.getString(CliParams.ID.getServerParam());
    UserResource user = usersApi.getUser(id);
    return objectWriter.writeValueAsString(fromUserResource(user));
  }

  private static String deleteUser(UsersApi usersApi, JSONObject json) throws ApiException {
    String id = json.getString(CliParams.ID.getServerParam());
    usersApi.deleteUser(id);
    return EMPTY;
  }

  private static User fromUserResource(UserResource userResource) {

    return new User()
        .id(userResource.getId())
        .name(userResource.getDisplayName())
        .externalId(userResource.getExternalId())
        .email(userResource.getEmails().get(0).getValue())
        .state(userResource.getActive() ? ENABLED : DISABLED)
        .createdAt(fromDateString(userResource.getMeta().getCreated()))
        .updatedAt(fromDateString(userResource.getMeta().getLastModified()));
  }

  private static Long fromDateString(String date) {
    // TODO: Should really try to get OpenAPI to convert from dates itself.
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    try {
      return sdf.parse(date).getTime();
    } catch (ParseException e) {
      return null;
    }
  }
}
