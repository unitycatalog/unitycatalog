package io.unitycatalog.server.sdk.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpStatus;
import io.unitycatalog.control.model.UserResource;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.service.Scim2GroupService;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests {@code /scim2/Groups}: SCIM group CRUD, filter, PATCH forms and membership rules. */
public class SdkScim2GroupTest extends SdkAccessControlBaseCRUDTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String GROUPS_PATH = "/api/1.0/unity-control/scim2/Groups";
  private static final String USERS_PATH = "/api/1.0/unity-control/scim2/Users";
  private static final String PATCH_OP = "urn:ietf:params:scim:api:messages:2.0:PatchOp";
  private static final String REGULAR_USER = "group-regular@localhost";

  private UserResource alice;
  private UserResource bob;
  private UserResource carol;

  @BeforeEach
  public void setUpUsers() throws Exception {
    alice = createTestUser("alice@localhost", "Alice");
    bob = createTestUser("bob@localhost", "Bob");
    carol = createTestUser("carol@localhost", "Carol");
  }

  @Test
  @SneakyThrows
  public void testCreateAndGetGroup() {
    HttpResponse<String> created =
        send(adminConfig, "POST", GROUPS_PATH, groupBody("data-eng", "okta-1", alice.getId()));
    assertThat(created.statusCode()).isEqualTo(201);
    assertThat(created.headers().firstValue("Content-Type").orElse(""))
        .startsWith("application/scim+json");
    JsonNode group = MAPPER.readTree(created.body());
    assertThat(group.get("displayName").asText()).isEqualTo("data-eng");
    assertThat(group.get("externalId").asText()).isEqualTo("okta-1");
    assertThat(memberIds(group)).containsExactly(alice.getId());

    HttpResponse<String> fetched =
        send(adminConfig, "GET", GROUPS_PATH + "/" + group.get("id").asText(), null);
    assertThat(fetched.statusCode()).isEqualTo(200);
    JsonNode fetchedGroup = MAPPER.readTree(fetched.body());
    assertThat(fetchedGroup.get("id")).isEqualTo(group.get("id"));
    assertThat(fetchedGroup.get("displayName").asText()).isEqualTo("data-eng");
    assertThat(memberIds(fetchedGroup)).containsExactly(alice.getId());

    // Without members the group is created empty.
    JsonNode empty = createGroup("empty");
    assertThat(empty.has("members") ? empty.get("members").size() : 0).isZero();
  }

  @Test
  @SneakyThrows
  public void testDuplicateNameIsConflict() {
    createGroup("taken");
    JsonNode other = createGroup("other");
    String otherPath = GROUPS_PATH + "/" + other.get("id").asText();
    String rename = patchBody(op("replace", "displayName", "\"taken\""));

    List<HttpResponse<String>> conflicts =
        List.of(
            send(adminConfig, "POST", GROUPS_PATH, groupBody("taken")),
            send(adminConfig, "PUT", otherPath, groupBody("taken")),
            send(adminConfig, "PATCH", otherPath, rename));

    for (HttpResponse<String> conflict : conflicts) {
      assertThat(conflict.statusCode()).isEqualTo(409);
      JsonNode error = MAPPER.readTree(conflict.body());
      assertThat(error.get("status").asText()).isEqualTo("409");
      assertThat(error.get("detail").asText()).contains("Group already exists: taken");
    }
    assertThat(groupNames(list(""))).containsExactly("other", "taken");
  }

  @Test
  @SneakyThrows
  public void testConcurrentCreatesOfOneNameYieldOneGroup() {
    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int round = 0; round < 5; round++) {
        String name = "race-" + round;
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Integer>> statuses = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
          statuses.add(
              pool.submit(
                  () -> {
                    start.await();
                    return send(adminConfig, "POST", GROUPS_PATH, groupBody(name)).statusCode();
                  }));
        }
        start.countDown();
        List<Integer> codes = new ArrayList<>();
        for (Future<Integer> status : statuses) {
          codes.add(status.get());
        }
        assertThat(codes.stream().filter(code -> code == 201)).hasSize(1);
        assertThat(codes.stream().filter(code -> code == 409)).hasSize(threads - 1);
        assertThat(groupNames(list("?filter=" + encode("displayName eq \"" + name + "\""))).size())
            .isEqualTo(1);
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  @SneakyThrows
  public void testInvalidMembersAreRejectedAndNothingIsApplied() {
    JsonNode other = createGroup("other");
    JsonNode group = createGroup("target", alice.getId());
    String groupPath = GROUPS_PATH + "/" + group.get("id").asText();
    String unknownId = UUID.randomUUID().toString();

    for (String invalid : List.of(unknownId, "not-a-uuid", other.get("id").asText())) {
      // A valid member listed next to the invalid one is not applied either.
      HttpResponse<String> create =
          send(
              adminConfig,
              "POST",
              GROUPS_PATH,
              groupBody("new-" + invalid, null, bob.getId(), invalid));
      HttpResponse<String> put =
          send(adminConfig, "PUT", groupPath, groupBody("renamed", null, bob.getId(), invalid));
      HttpResponse<String> patch =
          send(
              adminConfig,
              "PATCH",
              groupPath,
              patchBody(
                  op("replace", "displayName", "\"renamed\""),
                  op("add", "members", memberValues(bob.getId(), invalid))));

      for (HttpResponse<String> response : List.of(create, put, patch)) {
        assertThat(response.statusCode()).isEqualTo(400);
        JsonNode error = MAPPER.readTree(response.body());
        assertThat(error.get("status").asText()).isEqualTo("400");
        assertThat(error.get("detail").asText()).contains(invalid);
      }
      JsonNode unchanged = get(group.get("id").asText());
      assertThat(unchanged.get("displayName").asText()).isEqualTo("target");
      assertThat(memberIds(unchanged)).containsExactly(alice.getId());
    }
    assertThat(groupNames(list(""))).containsExactly("other", "target");
  }

  @Test
  @SneakyThrows
  public void testUnknownGroupIsNotFound() {
    for (String id : List.of(UUID.randomUUID().toString(), "not-a-uuid")) {
      String path = GROUPS_PATH + "/" + id;
      for (String method : List.of("GET", "PUT", "PATCH", "DELETE")) {
        String body =
            switch (method) {
              case "PUT" -> groupBody("x");
              case "PATCH" -> patchBody(op("replace", "displayName", "\"x\""));
              default -> null;
            };
        HttpResponse<String> response = send(adminConfig, method, path, body);
        assertThat(response.statusCode()).as(method + " " + id).isEqualTo(404);
        JsonNode error = MAPPER.readTree(response.body());
        assertThat(error.get("status").asText()).isEqualTo("404");
        assertThat(error.get("detail").asText()).contains("Group not found: " + id);
      }
    }
  }

  @Test
  @SneakyThrows
  public void testOnlyMetastoreOwnerCanManageGroups() {
    createTestUser(REGULAR_USER, "Regular");
    ServerConfig regular = createTestUserServerConfig(REGULAR_USER);
    String id = createGroup("owned").get("id").asText();
    String path = GROUPS_PATH + "/" + id;

    List<HttpResponse<String>> denied =
        List.of(
            send(regular, "POST", GROUPS_PATH, groupBody("nope")),
            send(regular, "GET", GROUPS_PATH, null),
            send(regular, "GET", path, null),
            send(regular, "PUT", path, groupBody("nope")),
            send(regular, "PATCH", path, patchBody(op("replace", "displayName", "\"nope\""))),
            send(regular, "DELETE", path, null));

    for (HttpResponse<String> response : denied) {
      assertThat(response.statusCode()).isEqualTo(403);
      assertThat(response.body()).contains("\"error_code\":\"PERMISSION_DENIED\"");
    }
    assertThat(groupNames(list(""))).containsExactly("owned");
  }

  @Test
  @SneakyThrows
  public void testListFilterAndPagination() {
    for (String name : List.of("g1", "g2", "g3", "g4", "g5")) {
      createGroup(name, alice.getId());
    }

    JsonNode all = list("");
    assertThat(all.get("totalResults").asInt()).isEqualTo(5);
    assertThat(groupNames(all)).containsExactly("g1", "g2", "g3", "g4", "g5");
    assertThat(memberIds(all.get("Resources").get(0))).containsExactly(alice.getId());

    // totalResults counts every match, not the page.
    JsonNode page = list("?startIndex=2&count=2");
    assertThat(groupNames(page)).containsExactly("g2", "g3");
    assertThat(page.get("totalResults").asInt()).isEqualTo(5);
    assertThat(page.get("startIndex").asInt()).isEqualTo(2);
    assertThat(page.get("itemsPerPage").asInt()).isEqualTo(2);
    assertThat(groupNames(list("?startIndex=5&count=10"))).containsExactly("g5");
    assertThat(groupNames(list("?startIndex=9"))).isEmpty();

    JsonNode filtered = list("?filter=" + encode("displayName eq \"g3\""));
    assertThat(groupNames(filtered)).containsExactly("g3");
    assertThat(filtered.get("totalResults").asInt()).isEqualTo(1);
    JsonNode none = list("?filter=" + encode("displayName eq \"missing\""));
    assertThat(none.get("totalResults").asInt()).isZero();

    JsonNode withoutMembers = list("?excludedAttributes=members");
    assertThat(withoutMembers.get("Resources").get(0).has("members")).isFalse();
    assertThat(
            MAPPER
                .readTree(
                    send(
                            adminConfig,
                            "GET",
                            GROUPS_PATH
                                + "/"
                                + all.get("Resources").get(0).get("id").asText()
                                + "?excludedAttributes=members",
                            null)
                        .body())
                .has("members"))
        .isFalse();

    HttpResponse<String> badFilter =
        send(adminConfig, "GET", GROUPS_PATH + "?filter=" + encode("displayName eq"), null);
    assertThat(badFilter.statusCode()).isEqualTo(400);
    assertThat(MAPPER.readTree(badFilter.body()).get("status").asText()).isEqualTo("400");
  }

  @Test
  @SneakyThrows
  public void testPutReplacesNameExternalIdAndMembers() {
    JsonNode group = createGroup("before", alice.getId(), bob.getId());
    String id = group.get("id").asText();

    HttpResponse<String> put =
        send(
            adminConfig,
            "PUT",
            GROUPS_PATH + "/" + id,
            groupBody("after", "ext-2", bob.getId(), carol.getId()));

    assertThat(put.statusCode()).isEqualTo(200);
    JsonNode replaced = get(id);
    assertThat(replaced.get("displayName").asText()).isEqualTo("after");
    assertThat(replaced.get("externalId").asText()).isEqualTo("ext-2");
    assertThat(memberIds(replaced)).containsExactlyInAnyOrder(bob.getId(), carol.getId());

    // Omitting members and externalId clears them.
    send(adminConfig, "PUT", GROUPS_PATH + "/" + id, groupBody("after"));
    JsonNode cleared = get(id);
    assertThat(cleared.has("externalId")).isFalse();
    assertThat(memberIds(cleared)).isEmpty();
  }

  @Test
  @SneakyThrows
  public void testPatchForms() {
    String id = createGroup("patched", alice.getId()).get("id").asText();
    String path = GROUPS_PATH + "/" + id;

    // {request ops, expected name, expected members after}
    record Step(String ops, String name, Set<String> members) {}
    List<Step> steps =
        List.of(
            // Okta add: path members, value list. Op names are case-insensitive.
            new Step(
                op("Add", "members", memberValues(bob.getId())),
                "patched",
                Set.of(alice.getId(), bob.getId())),
            // Re-adding a member changes nothing.
            new Step(
                op("add", "members", memberValues(alice.getId(), carol.getId())),
                "patched",
                Set.of(alice.getId(), bob.getId(), carol.getId())),
            // Okta remove: a filter on the member value.
            new Step(
                op("remove", "members[value eq \\\"" + bob.getId() + "\\\"]", null),
                "patched",
                Set.of(alice.getId(), carol.getId())),
            // Removing a non-member changes nothing.
            new Step(
                op("REMOVE", "members[value eq \\\"" + bob.getId() + "\\\"]", null),
                "patched",
                Set.of(alice.getId(), carol.getId())),
            new Step(
                op("replace", "members", memberValues(carol.getId())),
                "patched",
                Set.of(carol.getId())),
            // Replace the whole member list.
            new Step(
                op("replace", "members", memberValues(bob.getId(), carol.getId())),
                "patched",
                Set.of(bob.getId(), carol.getId())),
            // Okta rename: replace without a path, the value is {id, displayName}.
            new Step(
                op("replace", null, "{\"id\":\"" + id + "\",\"displayName\":\"renamed\"}"),
                "renamed",
                Set.of(bob.getId(), carol.getId())),
            // Replace by path.
            new Step(
                op("replace", "displayName", "\"renamed-again\""),
                "renamed-again",
                Set.of(bob.getId(), carol.getId())),
            // Replace with no members.
            new Step(op("replace", "members", "[]"), "renamed-again", Set.of()));

    for (Step step : steps) {
      HttpResponse<String> response = send(adminConfig, "PATCH", path, patchBody(step.ops()));
      assertThat(response.statusCode()).as(step.ops()).isEqualTo(200);
      JsonNode returned = MAPPER.readTree(response.body());
      assertThat(returned.get("displayName").asText()).as(step.ops()).isEqualTo(step.name());
      assertThat(memberIds(returned)).as(step.ops()).isEqualTo(step.members());
      assertThat(memberIds(get(id))).as(step.ops()).isEqualTo(step.members());
    }
  }

  @Test
  @SneakyThrows
  public void testUnsupportedPatchAppliesNothing() {
    String id = createGroup("stable", alice.getId()).get("id").asText();
    String path = GROUPS_PATH + "/" + id;

    // A supported rename next to an unsupported operation: the whole request is refused.
    HttpResponse<String> response =
        send(
            adminConfig,
            "PATCH",
            path,
            patchBody(
                op("replace", "displayName", "\"changed\""), op("replace", "nickName", "\"x\"")));

    assertThat(response.statusCode()).isEqualTo(501);
    assertThat(MAPPER.readTree(response.body()).get("status").asText()).isEqualTo("501");
    assertThat(get(id).get("displayName").asText()).isEqualTo("stable");

    // A remove without a filter is refused too: it would otherwise remove every member.
    HttpResponse<String> removeAll =
        send(
            adminConfig,
            "PATCH",
            path,
            patchBody(op("remove", "members", memberValues(alice.getId()))));
    assertThat(removeAll.statusCode()).isEqualTo(501);
    assertThat(memberIds(get(id))).containsExactly(alice.getId());
  }

  @Test
  @SneakyThrows
  public void testDeleteGroupRemovesItAndKeepsUsers() {
    String id = createGroup("doomed", alice.getId()).get("id").asText();
    String path = GROUPS_PATH + "/" + id;

    HttpResponse<String> deleted = send(adminConfig, "DELETE", path, null);

    assertThat(deleted.statusCode()).isEqualTo(204);
    for (String method : List.of("GET", "DELETE")) {
      assertThat(send(adminConfig, method, path, null).statusCode()).isEqualTo(404);
    }
    assertThat(send(adminConfig, "PUT", path, groupBody("doomed")).statusCode()).isEqualTo(404);
    assertThat(
            send(adminConfig, "PATCH", path, patchBody(op("replace", "displayName", "\"x\"")))
                .statusCode())
        .isEqualTo(404);
    assertThat(groupNames(list(""))).isEmpty();
    assertThat(send(adminConfig, "GET", USERS_PATH + "/" + alice.getId(), null).statusCode())
        .isEqualTo(200);
    // The name is free again.
    assertThat(send(adminConfig, "POST", GROUPS_PATH, groupBody("doomed")).statusCode())
        .isEqualTo(201);
  }

  @Test
  @SneakyThrows
  public void testDeleteGroupClearsGrantsBeforeTheGroupIsGone() {
    Repositories repositories =
        new Repositories(
            hibernateConfigurator.getSessionFactory(), new ServerProperties(serverProperties));
    String id = createGroup("granted", alice.getId()).get("id").asText();
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    List<Integer> statusWhenCleared = new ArrayList<>();
    when(authorizer.clearAuthorizationsForPrincipal(any()))
        .thenAnswer(
            call -> {
              statusWhenCleared.add(
                  send(adminConfig, "GET", GROUPS_PATH + "/" + id, null).statusCode());
              return true;
            });

    HttpStatus status =
        new Scim2GroupService(authorizer, repositories).deleteGroup(id).aggregate().join().status();

    verify(authorizer).clearAuthorizationsForPrincipal(UUID.fromString(id));
    assertThat(status).isEqualTo(HttpStatus.NO_CONTENT);
    assertThat(statusWhenCleared).containsExactly(200);
    assertThat(send(adminConfig, "GET", GROUPS_PATH + "/" + id, null).statusCode()).isEqualTo(404);
  }

  @Test
  @SneakyThrows
  public void testUserDeleteDropsMembershipsButDeactivateKeepsThem() {
    String id = createGroup("members", alice.getId(), bob.getId()).get("id").asText();

    String deactivate =
        "{\"schemas\":[\""
            + PATCH_OP
            + "\"],\"Operations\":[{\"op\":\"replace\","
            + "\"value\":{\"active\":false}}]}";
    HttpResponse<String> deactivated =
        send(adminConfig, "PATCH", USERS_PATH + "/" + alice.getId(), deactivate);
    assertThat(deactivated.statusCode()).isEqualTo(200);
    assertThat(memberIds(get(id))).containsExactlyInAnyOrder(alice.getId(), bob.getId());

    HttpResponse<String> deleted =
        send(adminConfig, "DELETE", USERS_PATH + "/" + alice.getId(), null);
    assertThat(deleted.statusCode()).isEqualTo(200);
    assertThat(memberIds(get(id))).containsExactly(bob.getId());
  }

  private HttpResponse<String> send(ServerConfig config, String method, String path, String body)
      throws Exception {
    return TestUtils.sendRaw(config, method, path, Optional.ofNullable(body));
  }

  private JsonNode createGroup(String name, String... memberIds) throws Exception {
    HttpResponse<String> response =
        send(adminConfig, "POST", GROUPS_PATH, groupBody(name, null, memberIds));
    assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
    return MAPPER.readTree(response.body());
  }

  private JsonNode get(String id) throws Exception {
    HttpResponse<String> response = send(adminConfig, "GET", GROUPS_PATH + "/" + id, null);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return MAPPER.readTree(response.body());
  }

  private JsonNode list(String query) throws Exception {
    HttpResponse<String> response = send(adminConfig, "GET", GROUPS_PATH + query, null);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return MAPPER.readTree(response.body());
  }

  private static String encode(String value) {
    return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8);
  }

  /** A group body; each of {@code memberIds} is used as given, so a test can pass invalid ones. */
  private static String groupBody(String name, String externalId, String... memberIds) {
    String members = memberIds.length == 0 ? "" : ",\"members\":" + memberValues(memberIds);
    String external = externalId == null ? "" : ",\"externalId\":\"" + externalId + "\"";
    return "{\"displayName\":\"" + name + "\"" + external + members + "}";
  }

  private static String groupBody(String name) {
    return groupBody(name, null);
  }

  private static String memberValues(String... memberIds) {
    return List.of(memberIds).stream()
        .map(memberId -> "{\"value\":\"" + memberId + "\"}")
        .collect(Collectors.joining(",", "[", "]"));
  }

  private static String op(String op, String path, String value) {
    return "{\"op\":\""
        + op
        + "\""
        + (path == null ? "" : ",\"path\":\"" + path + "\"")
        + (value == null ? "" : ",\"value\":" + value)
        + "}";
  }

  private static String patchBody(String... ops) {
    return "{\"schemas\":[\"" + PATCH_OP + "\"],\"Operations\":[" + String.join(",", ops) + "]}";
  }

  private static Set<String> memberIds(JsonNode group) {
    return group.has("members")
        ? StreamSupport.stream(group.get("members").spliterator(), false)
            .map(member -> member.get("value").asText())
            .collect(Collectors.toSet())
        : Set.of();
  }

  private static List<String> groupNames(JsonNode list) {
    return list.has("Resources")
        ? StreamSupport.stream(list.get("Resources").spliterator(), false)
            .map(group -> group.get("displayName").asText())
            .collect(Collectors.toList())
        : List.of();
  }
}
