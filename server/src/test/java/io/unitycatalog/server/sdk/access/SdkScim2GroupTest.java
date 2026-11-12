package io.unitycatalog.server.sdk.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.control.model.User;
import io.unitycatalog.control.model.UserResource;
import io.unitycatalog.server.auth.JCasbinAuthorizer;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.dao.GroupMemberDAO;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.testdatabase.SupportedDatabase;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.Transaction;
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
  public void testRefusedPatchAppliesNothing() {
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

    // {operation, status, detail prefix}: each is refused alone and changes nothing.
    String bobFilter = "members[value eq \\\"" + bob.getId() + "\\\"]";
    List<List<Object>> refused =
        List.of(
            // A members value that is not an array would read as no members at all.
            List.of(op("replace", "members", "\"x\""), 400, "members needs an array value."),
            List.of(op("add", "members", "\"x\""), 400, "members needs an array value."),
            List.of(op("replace", "displayName", "{\"a\":1}"), 400, "Invalid PATCH value: "),
            List.of(op("replace", "displayName", "[]"), 400, "Invalid PATCH value: "),
            List.of(op("add", "members", "[{\"value\":{\"a\":1}}]"), 400, "Invalid PATCH value: "),
            List.of(
                op("add", "members", "[\"" + bob.getId() + "\"]"), 400, "Invalid PATCH value: "),
            List.of(op("add", "members", "[null]"), 400, "Member is not an existing user: null"),
            List.of(
                op("add", bobFilter, memberValues(bob.getId())),
                501,
                "PATCH is not supported for: "),
            List.of(
                op("replace", null, "{\"externalId\":\"x\"}"),
                501,
                "PATCH is not supported for: [externalId]"));
    for (List<Object> row : refused) {
      String operation = (String) row.get(0);
      HttpResponse<String> refusal = send(adminConfig, "PATCH", path, patchBody(operation));
      assertThat(refusal.statusCode())
          .as(operation + " -> " + refusal.body())
          .isEqualTo(row.get(1));
      assertThat(MAPPER.readTree(refusal.body()).get("detail").asText())
          .as(operation)
          .startsWith((String) row.get(2))
          .doesNotContain("com.unboundid");
      JsonNode group = get(id);
      assertThat(group.get("displayName").asText()).as(operation).isEqualTo("stable");
      assertThat(group.has("externalId")).as(operation).isFalse();
      assertThat(memberIds(group)).as(operation).containsExactly(alice.getId());
    }
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
  public void testDeleteGroupClearsItsGrants() {
    String id = createGroup("granted", alice.getId()).get("id").asText();
    UUID groupId = UUID.fromString(id);
    UUID metastoreId =
        new Repositories(
                hibernateConfigurator.getSessionFactory(), new ServerProperties(serverProperties))
            .getMetastoreRepository()
            .getMetastoreId();
    // A grant to the group id, written through another authorizer over the server's store.
    try (JCasbinAuthorizer granting = newAuthorizer()) {
      granting.grantAuthorization(groupId, metastoreId, Privileges.CREATE_CATALOG);
    }
    try (JCasbinAuthorizer before = newAuthorizer()) {
      assertThat(before.listAuthorizations(groupId, metastoreId))
          .containsExactly(Privileges.CREATE_CATALOG);
    }

    assertThat(send(adminConfig, "DELETE", GROUPS_PATH + "/" + id, null).statusCode())
        .isEqualTo(204);

    try (JCasbinAuthorizer after = newAuthorizer()) {
      assertThat(after.listAuthorizations(groupId, metastoreId)).isEmpty();
    }
    assertThat(send(adminConfig, "GET", GROUPS_PATH + "/" + id, null).statusCode()).isEqualTo(404);
  }

  /**
   * A user deleted at the same time as it is added to a group must not stay a member. The delete
   * here removes the memberships and then the user row (as the hard delete of a user will), and is
   * held uncommitted while the add arrives. The add has to wait for it and then find no user.
   */
  @Test
  @SneakyThrows
  public void testAddRacingAUserDeleteLeavesNoMembership() {
    String userId = createTestUser("race-del@localhost", "RaceDel").getId();
    String id = createGroup("race-del").get("id").asText();
    UUID uid = UUID.fromString(userId);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (Session deleting = hibernateConfigurator.getSessionFactory().openSession()) {
      Transaction tx = deleting.beginTransaction();
      deleting
          .createMutationQuery("DELETE FROM GroupMemberDAO WHERE userId = :userId")
          .setParameter("userId", uid)
          .executeUpdate();
      deleting
          .createMutationQuery("DELETE FROM UserDAO WHERE id = :userId")
          .setParameter("userId", uid)
          .executeUpdate();
      Future<HttpResponse<String>> add =
          pool.submit(
              () ->
                  send(
                      adminConfig,
                      "PATCH",
                      GROUPS_PATH + "/" + id,
                      patchBody(op("add", "members", memberValues(userId)))));
      // Without the lock the add finishes at once, before the delete commits. The hold stays
      // short, as H2 gives up on a lock it waits for too long.
      try {
        add.get(500, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // The add is waiting for the delete, as it should.
      }
      tx.commit();

      HttpResponse<String> response = add.get(30, TimeUnit.SECONDS);
      assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
      assertThat(MAPPER.readTree(response.body()).get("detail").asText())
          .contains("Member is not an existing user: " + userId);
    } finally {
      pool.shutdownNow();
    }
    assertThat(memberIds(get(id))).isEmpty();
  }

  /**
   * A user delete racing an add of that user to a group must not deadlock with it. A reader locks
   * the group's memberships, so on MySQL the add waits there after reading the user row, and the
   * delete arrives meanwhile. The delete writes the user row first, so it waits for the add;
   * dropping the memberships first would deadlock the two.
   */
  @Test
  @SneakyThrows
  public void testUserDeleteRacingAnAddDoesNotDeadlock() {
    String userId = createTestUser("race-disable@localhost", "RaceDisable").getId();
    createGroup("race-disable-other", userId);
    String id = createGroup("race-disable").get("id").asText();
    UserRepository userRepository =
        new UserRepository(null, hibernateConfigurator.getSessionFactory());
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try (Session reading = hibernateConfigurator.getSessionFactory().openSession()) {
      Transaction tx = reading.beginTransaction();
      reading
          .createSelectionQuery(
              "FROM GroupMemberDAO WHERE groupId = :groupId", GroupMemberDAO.class)
          .setParameter("groupId", UUID.fromString(id))
          .setHibernateLockMode(LockMode.PESSIMISTIC_WRITE)
          .list();
      Future<HttpResponse<String>> add =
          pool.submit(
              () ->
                  send(
                      adminConfig,
                      "PATCH",
                      GROUPS_PATH + "/" + id,
                      patchBody(op("add", "members", memberValues(userId)))));
      // The delete starts once the add waits for the lock, and the lock is released once the
      // delete waits for the add.
      awaitWaitingOrDone(add, 1);
      Future<?> delete = pool.submit(() -> userRepository.deleteUser(userId));
      awaitWaitingOrDone(delete, 2);
      tx.commit();

      HttpResponse<String> response = add.get(30, TimeUnit.SECONDS);
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      delete.get(30, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
    assertThat(userRepository.getUser(userId).getState()).isEqualTo(User.StateEnum.DISABLED);
    assertThat(memberIds(get(id))).isEmpty();
  }

  /**
   * Okta pushes one group, in the order and with the bodies of its SCIM 2.0 guide: look the group
   * up by name, create it, add members (with display names), add and remove members in one request,
   * rename it (a replace without a path), remove a member, delete it. Okta takes a 200 or a 204 for
   * each PATCH.
   */
  @Test
  @SneakyThrows
  public void testOktaGroupPushJourney() {
    String groupSchema = "urn:ietf:params:scim:schemas:core:2.0:Group";
    String lookupQuery =
        "?filter=" + encode("displayName eq \"Test SCIMv2\"") + "&startIndex=1&count=100";

    // Okta looks for the group first: there is none.
    JsonNode none = list(lookupQuery);
    assertThat(none.get("totalResults").asInt()).isZero();
    assertThat(groupNames(none)).isEmpty();

    // Create the group, with no members.
    HttpResponse<String> created =
        send(
            adminConfig,
            "POST",
            GROUPS_PATH,
            "{\"schemas\":[\""
                + groupSchema
                + "\"],\"displayName\":\"Test SCIMv2\",\"members\":[]}");
    assertThat(created.statusCode()).isEqualTo(201);
    JsonNode group = MAPPER.readTree(created.body());
    String id = group.get("id").asText();
    assertThat(group.get("displayName").asText()).isEqualTo("Test SCIMv2");
    assertThat(memberIds(group)).isEmpty();
    assertThat(group.get("meta").get("resourceType").asText()).isEqualTo("Group");
    assertThat(groupNames(list(lookupQuery))).containsExactly("Test SCIMv2");

    // Add two members.
    patchOkta(
        id,
        op(
            "add",
            "members",
            "[{\"value\":\""
                + alice.getId()
                + "\",\"display\":\"alice@localhost\"},"
                + "{\"value\":\""
                + bob.getId()
                + "\",\"display\":\"bob@localhost\"}]"));
    assertThat(memberIds(get(id))).containsExactlyInAnyOrder(alice.getId(), bob.getId());

    // Add and remove members in one request.
    patchOkta(
        id,
        op(
            "add",
            "members",
            "[{\"value\":\"" + carol.getId() + "\",\"display\":\"carol@localhost\"}]"),
        op("remove", "members[value eq \\\"" + alice.getId() + "\\\"]", null));
    assertThat(memberIds(get(id))).containsExactlyInAnyOrder(bob.getId(), carol.getId());

    // Rename.
    patchOkta(
        id, op("replace", null, "{\"id\":\"" + id + "\",\"displayName\":\"Test SCIMv2 renamed\"}"));
    JsonNode renamed = get(id);
    assertThat(renamed.get("displayName").asText()).isEqualTo("Test SCIMv2 renamed");
    assertThat(memberIds(renamed)).containsExactlyInAnyOrder(bob.getId(), carol.getId());
    assertThat(list(lookupQuery).get("totalResults").asInt()).isZero();

    // Remove one member.
    patchOkta(id, op("remove", "members[value eq \\\"" + bob.getId() + "\\\"]", null));
    assertThat(memberIds(get(id))).containsExactly(carol.getId());

    // Unlink and delete the group.
    assertThat(send(adminConfig, "DELETE", GROUPS_PATH + "/" + id, null).statusCode())
        .isEqualTo(204);
    assertThat(send(adminConfig, "GET", GROUPS_PATH + "/" + id, null).statusCode()).isEqualTo(404);
    assertThat(groupNames(list(""))).isEmpty();
    // The users stay.
    for (UserResource user : List.of(alice, bob, carol)) {
      assertThat(send(adminConfig, "GET", USERS_PATH + "/" + user.getId(), null).statusCode())
          .isEqualTo(200);
    }
  }

  /** Sends an Okta PATCH; Okta accepts a 200 or a 204. */
  @SneakyThrows
  private void patchOkta(String groupId, String... ops) {
    HttpResponse<String> response =
        send(adminConfig, "PATCH", GROUPS_PATH + "/" + groupId, patchBody(ops));
    assertThat(response.statusCode()).as(response.body()).isIn(200, 204);
  }

  /**
   * Waits until {@code request} is done or the database reports at least {@code blocked} sessions
   * waiting for a lock. A request that finds nothing to wait for, as on H2 and PostgreSQL where the
   * reader locks no row of a group without members, is done before it is seen waiting.
   */
  @SneakyThrows
  private void awaitWaitingOrDone(Future<?> request, int blocked) {
    String sql =
        switch (SupportedDatabase.selected()) {
          case H2 ->
              "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SESSIONS WHERE BLOCKER_ID IS NOT NULL";
          case POSTGRES_18 ->
              "SELECT COUNT(*) FROM pg_stat_activity"
                  + " WHERE datname = current_database() AND wait_event_type = 'Lock'";
          case MYSQL_8 ->
              "SELECT COUNT(DISTINCT REQUESTING_ENGINE_TRANSACTION_ID)"
                  + " FROM performance_schema.data_lock_waits";
        };
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      if (request.isDone()) {
        return;
      }
      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        if (((Number) session.createNativeQuery(sql, Object.class).getSingleResult()).intValue()
            >= blocked) {
          return;
        }
      }
      Thread.sleep(10);
    }
    fail("The request is neither done nor waiting for a lock after 10s");
  }

  /** An authorizer over the same store as the server's. */
  private JCasbinAuthorizer newAuthorizer() throws Exception {
    return new JCasbinAuthorizer(hibernateConfigurator, new ServerProperties(serverProperties));
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

  @Test
  @SneakyThrows
  public void testOverlongNamesAreRejected() {
    String id = createGroup("short").get("id").asText();
    String path = GROUPS_PATH + "/" + id;
    String tooLong = "x".repeat(256);

    // {response, the attribute the message must name}
    List<Map.Entry<HttpResponse<String>, String>> responses =
        List.of(
            Map.entry(send(adminConfig, "POST", GROUPS_PATH, groupBody(tooLong)), "displayName"),
            Map.entry(
                send(adminConfig, "POST", GROUPS_PATH, groupBody("ext", tooLong)), "externalId"),
            Map.entry(send(adminConfig, "PUT", path, groupBody(tooLong)), "displayName"),
            Map.entry(send(adminConfig, "PUT", path, groupBody("ext", tooLong)), "externalId"),
            Map.entry(
                send(
                    adminConfig,
                    "PATCH",
                    path,
                    patchBody(op("replace", "displayName", "\"" + tooLong + "\""))),
                "displayName"),
            Map.entry(
                send(
                    adminConfig,
                    "PATCH",
                    path,
                    patchBody(op("replace", null, "{\"displayName\":\"" + tooLong + "\"}"))),
                "displayName"));

    for (Map.Entry<HttpResponse<String>, String> response : responses) {
      assertThat(response.getKey().statusCode()).as(response.getValue()).isEqualTo(400);
      JsonNode error = MAPPER.readTree(response.getKey().body());
      assertThat(error.get("detail").asText())
          .isEqualTo(response.getValue() + " is longer than 255 characters.");
    }
    // The longest value is accepted.
    assertThat(
            send(adminConfig, "POST", GROUPS_PATH, groupBody("y".repeat(255), "e".repeat(255)))
                .statusCode())
        .isEqualTo(201);
    assertThat(groupNames(list(""))).hasSize(2).contains("short");
  }

  @Test
  @SneakyThrows
  public void testMissingOrBlankDisplayNameIsRejected() {
    String id = createGroup("named", alice.getId()).get("id").asText();
    String path = GROUPS_PATH + "/" + id;

    // {form, response}: a missing or blank displayName is refused by POST, PUT and PATCH alike.
    List<Map.Entry<String, HttpResponse<String>>> responses =
        List.of(
            Map.entry("POST without", send(adminConfig, "POST", GROUPS_PATH, "{}")),
            Map.entry("POST blank", send(adminConfig, "POST", GROUPS_PATH, groupBody("  "))),
            Map.entry("PUT without", send(adminConfig, "PUT", path, "{}")),
            Map.entry("PUT blank", send(adminConfig, "PUT", path, groupBody(""))),
            Map.entry(
                "PATCH blank",
                send(adminConfig, "PATCH", path, patchBody(op("replace", "displayName", "\"\"")))),
            Map.entry(
                "PATCH without",
                send(
                    adminConfig, "PATCH", path, patchBody(op("replace", null, "{\"id\":\"x\"}")))));

    for (Map.Entry<String, HttpResponse<String>> response : responses) {
      assertThat(response.getValue().statusCode())
          .as(response.getKey() + ": " + response.getValue().body())
          .isEqualTo(400);
      assertThat(MAPPER.readTree(response.getValue().body()).get("detail").asText())
          .as(response.getKey())
          .isEqualTo("displayName is required.");
    }
    JsonNode group = get(id);
    assertThat(group.get("displayName").asText()).isEqualTo("named");
    assertThat(memberIds(group)).containsExactly(alice.getId());
    assertThat(groupNames(list(""))).containsExactly("named");
  }

  @Test
  @SneakyThrows
  public void testPatchWithoutOperationsIsRejected() {
    String id = createGroup("patched", alice.getId()).get("id").asText();
    String path = GROUPS_PATH + "/" + id;

    HttpResponse<String> empty = send(adminConfig, "PATCH", path, patchBody());
    assertThat(empty.statusCode()).as(empty.body()).isEqualTo(400);
    JsonNode error = MAPPER.readTree(empty.body());
    assertThat(error.get("status").asText()).isEqualTo("400");
    assertThat(error.get("detail").asText()).isEqualTo("PATCH has no operations.");

    // A body without Operations is refused while it is read, before the service sees it.
    HttpResponse<String> missing =
        send(adminConfig, "PATCH", path, "{\"schemas\":[\"" + PATCH_OP + "\"]}");
    assertThat(missing.statusCode()).as(missing.body()).isEqualTo(400);

    JsonNode group = get(id);
    assertThat(group.get("displayName").asText()).isEqualTo("patched");
    assertThat(memberIds(group)).containsExactly(alice.getId());
  }

  @Test
  @SneakyThrows
  public void testPatchNamesAndIdsAreCaseInsensitive() {
    String id = createGroup("insensitive", alice.getId(), bob.getId()).get("id").asText();
    String path = GROUPS_PATH + "/" + id;

    // {ops, expected name, expected members}: attribute names are case-insensitive (RFC 7643, 2.1)
    // and so is the id in a member filter.
    record Step(String ops, String name, Set<String> members) {}
    List<Step> steps =
        List.of(
            new Step(
                op("add", "Members", memberValues(carol.getId())),
                "insensitive",
                Set.of(alice.getId(), bob.getId(), carol.getId())),
            new Step(
                op("replace", "DisplayName", "\"renamed\""),
                "renamed",
                Set.of(alice.getId(), bob.getId(), carol.getId())),
            new Step(
                op("replace", null, "{\"ID\":\"" + id + "\",\"DisplayName\":\"renamed-2\"}"),
                "renamed-2",
                Set.of(alice.getId(), bob.getId(), carol.getId())),
            new Step(
                op("remove", "members[value eq \\\"" + bob.getId().toUpperCase() + "\\\"]", null),
                "renamed-2",
                Set.of(alice.getId(), carol.getId())),
            new Step(
                op("remove", "MEMBERS[Value eq \\\"" + carol.getId() + "\\\"]", null),
                "renamed-2",
                Set.of(alice.getId())));

    for (Step step : steps) {
      HttpResponse<String> response = send(adminConfig, "PATCH", path, patchBody(step.ops()));
      assertThat(response.statusCode()).as(step.ops()).isEqualTo(200);
      JsonNode returned = MAPPER.readTree(response.body());
      assertThat(returned.get("displayName").asText()).as(step.ops()).isEqualTo(step.name());
      assertThat(memberIds(returned)).as(step.ops()).isEqualTo(step.members());
    }
  }

  @Test
  @SneakyThrows
  public void testConcurrentPatchesLoseNoMember() {
    int threads = 8;
    List<String> users = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      users.add(createTestUser("racer-" + i + "@localhost", "Racer" + i).getId());
    }
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int round = 0; round < 5; round++) {
        String id = createGroup("concurrent-" + round).get("id").asText();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<HttpResponse<String>>> responses = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
          // Every request adds the shared member and one of its own.
          String body = patchBody(op("add", "members", memberValues(alice.getId(), users.get(i))));
          responses.add(
              pool.submit(
                  () -> {
                    start.await();
                    return send(adminConfig, "PATCH", GROUPS_PATH + "/" + id, body);
                  }));
        }
        start.countDown();
        for (Future<HttpResponse<String>> response : responses) {
          assertThat(response.get().statusCode()).as(response.get().body()).isEqualTo(200);
        }
        Set<String> expected = new HashSet<>(users);
        expected.add(alice.getId());
        assertThat(memberIds(get(id))).as("round " + round).isEqualTo(expected);
      }
    } finally {
      pool.shutdownNow();
    }
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
