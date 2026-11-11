package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.ADD_MEMBER;
import static io.unitycatalog.cli.utils.CliUtils.CREATE;
import static io.unitycatalog.cli.utils.CliUtils.DELETE;
import static io.unitycatalog.cli.utils.CliUtils.EMPTY;
import static io.unitycatalog.cli.utils.CliUtils.GET;
import static io.unitycatalog.cli.utils.CliUtils.GROUP;
import static io.unitycatalog.cli.utils.CliUtils.LIST;
import static io.unitycatalog.cli.utils.CliUtils.REMOVE_MEMBER;
import static io.unitycatalog.cli.utils.CliUtils.UPDATE;
import static io.unitycatalog.cli.utils.CliUtils.postProcessAndPrintOutput;
import static io.unitycatalog.cli.utils.CliUtils.printEntityHelp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.control.ApiClient;
import io.unitycatalog.control.ApiException;
import io.unitycatalog.control.api.GroupsApi;
import io.unitycatalog.control.model.GroupMember;
import io.unitycatalog.control.model.GroupResource;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

public class GroupCli {
  private static final String PATCH_OP_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:PatchOp";
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    GroupsApi groupsApi = new GroupsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = EMPTY;
    switch (subCommand) {
      case CREATE:
        output = createGroup(groupsApi, json);
        break;
      case UPDATE:
        output = updateGroup(groupsApi, json);
        break;
      case LIST:
        output = listGroups(groupsApi, json);
        break;
      case GET:
        output = getGroup(groupsApi, json);
        break;
      case DELETE:
        output = deleteGroup(groupsApi, json);
        break;
      case ADD_MEMBER:
        output = patchGroup(groupsApi, json, "add", "members", memberValue(json));
        break;
      case REMOVE_MEMBER:
        output =
            patchGroup(
                groupsApi,
                json,
                "remove",
                "members[value eq \"" + json.getString(CliParams.MEMBER.getServerParam()) + "\"]",
                null);
        break;
      default:
        printEntityHelp(GROUP);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String createGroup(GroupsApi groupsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    GroupResource groupResource =
        new GroupResource()
            .displayName(json.getString(CliParams.NAME.getServerParam()))
            .externalId(json.optString(CliParams.EXTERNAL_ID.getServerParam(), null))
            .members(
                Arrays.stream(json.optString(CliParams.MEMBERS.getServerParam(), "").split(","))
                    .map(String::trim)
                    .filter(id -> !id.isEmpty())
                    .map(id -> new GroupMember().value(id))
                    .collect(Collectors.toList()));
    return objectWriter.writeValueAsString(fromGroupResource(groupsApi.createGroup(groupResource)));
  }

  /** A single PATCH, so a rename cannot overwrite a concurrent change to the members. */
  private static String updateGroup(GroupsApi groupsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    return patchGroup(
        groupsApi, json, "replace", "displayName", json.getString(CliParams.NAME.getServerParam()));
  }

  private static String listGroups(GroupsApi groupsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String filter = json.optString(CliParams.FILTER.getServerParam(), null);
    int startIndex = json.optInt(CliParams.START_INDEX.getServerParam(), 1);
    int count = json.optInt(CliParams.COUNT.getServerParam(), 100);
    List<Map<String, Object>> groups =
        groupsApi.listGroups(filter, startIndex, count).getResources().stream()
            .map(GroupCli::fromGroupResource)
            .collect(Collectors.toList());
    return objectWriter.writeValueAsString(groups);
  }

  private static String getGroup(GroupsApi groupsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String id = json.getString(CliParams.ID.getServerParam());
    return objectWriter.writeValueAsString(fromGroupResource(groupsApi.getGroup(id)));
  }

  private static String deleteGroup(GroupsApi groupsApi, JSONObject json) throws ApiException {
    groupsApi.deleteGroup(json.getString(CliParams.ID.getServerParam()));
    return EMPTY;
  }

  private static List<Map<String, String>> memberValue(JSONObject json) {
    return List.of(Map.of("value", json.getString(CliParams.MEMBER.getServerParam())));
  }

  /** Sends one SCIM PATCH operation (RFC 7644, section 3.5.2) and returns the changed group. */
  private static String patchGroup(
      GroupsApi groupsApi, JSONObject json, String op, String path, Object value)
      throws JsonProcessingException, ApiException {
    Map<String, Object> operation = new LinkedHashMap<>();
    operation.put("op", op);
    operation.put("path", path);
    if (value != null) {
      operation.put("value", value);
    }
    Map<String, Object> patch =
        Map.of("schemas", List.of(PATCH_OP_SCHEMA), "Operations", List.of(operation));
    GroupResource group =
        groupsApi.patchGroup(json.getString(CliParams.ID.getServerParam()), patch);
    return objectWriter.writeValueAsString(fromGroupResource(group));
  }

  private static Map<String, Object> fromGroupResource(GroupResource groupResource) {
    Map<String, Object> group = new LinkedHashMap<>();
    group.put("id", groupResource.getId());
    group.put("name", groupResource.getDisplayName());
    group.put("external_id", groupResource.getExternalId());
    group.put(
        "members",
        groupResource.getMembers().stream()
            .map(GroupMember::getValue)
            .collect(Collectors.toList()));
    group.put("created_at", UserCli.fromDateString(groupResource.getMeta().getCreated()));
    group.put("updated_at", UserCli.fromDateString(groupResource.getMeta().getLastModified()));
    return group;
  }
}
