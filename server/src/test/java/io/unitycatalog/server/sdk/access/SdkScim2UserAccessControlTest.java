package io.unitycatalog.server.sdk.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpStatus;
import io.unitycatalog.control.ApiException;
import io.unitycatalog.control.api.UsersApi;
import io.unitycatalog.control.model.UserResource;
import io.unitycatalog.control.model.UserResourceList;
import io.unitycatalog.server.auth.AllowingAuthorizer;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.service.Scim2UserService;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Properties;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class SdkScim2UserAccessControlTest extends SdkAccessControlBaseCRUDTest {
  private static final String REGULAR_USER = "scim-regular@localhost";
  private static final String OTHER_USER = "scim-other@localhost";
  private static final String SCIM_USERS_PATH = "/api/1.0/unity-control/scim2/Users";
  private static final String DEACTIVATE =
      "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],"
          + "\"Operations\":[{\"op\":\"replace\",\"value\":{\"active\":false}}]}";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String OWNER_REFUSED =
      "The metastore owner can't be deleted or deactivated: admin";

  private UsersApi regularUsersApi;
  private ServerConfig regularUserConfig;
  private UserResource otherUser;

  @BeforeEach
  public void setUpUsers() throws Exception {
    createTestUser(REGULAR_USER, "Regular User");
    otherUser = createTestUser(OTHER_USER, "Other User");

    regularUserConfig = createTestUserServerConfig(REGULAR_USER);
    regularUsersApi = new UsersApi(createControlApiClient(regularUserConfig));
  }

  @Test
  @SneakyThrows
  public void testOnlyMetastoreAdminCanListGetAndPatchUsers() {
    // Admin can list and get any user; a regular user can do neither.
    UserResourceList users = usersApi.listUsers(null, null, null);
    assertThat(users.getResources()).extracting(UserResource::getId).contains(otherUser.getId());

    UserResource fetchedUser = usersApi.getUser(otherUser.getId());
    assertThat(fetchedUser.getId()).isEqualTo(otherUser.getId());
    assertThat(fetchedUser.getEmails().get(0).getValue()).isEqualTo(OTHER_USER);

    assertScimPermissionDenied(() -> regularUsersApi.listUsers(null, null, null));
    assertScimPermissionDenied(() -> regularUsersApi.getUser(otherUser.getId()));

    // PATCH /scim2/Users/{id} is an Okta-style activate/deactivate. It is not modeled in the
    // control client, so it is exercised over raw HTTP. Like its sibling SCIM endpoints it requires
    // metastore OWNER -- before it was wired up the handler carried no authorization expression at
    // all.
    String patchPath = SCIM_USERS_PATH + "/" + otherUser.getId();

    // A regular user cannot patch another user.
    HttpResponse<String> denied =
        TestUtils.sendRaw(regularUserConfig, "PATCH", patchPath, Optional.of(DEACTIVATE));
    assertThat(denied.statusCode()).isEqualTo(403);
    assertThat(denied.body()).contains("\"error_code\":\"PERMISSION_DENIED\"");

    // The metastore admin can, and the user ends up deactivated.
    HttpResponse<String> allowed =
        TestUtils.sendRaw(adminConfig, "PATCH", patchPath, Optional.of(DEACTIVATE));
    assertThat(allowed.statusCode()).isEqualTo(200);
    assertThat(usersApi.getUser(otherUser.getId()).getActive()).isFalse();
  }

  @Test
  @SneakyThrows
  public void testMetastoreOwnerCannotBeDeletedOrDeactivated() {
    UserResource owner =
        usersApi.listUsers(null, null, null).getResources().stream()
            .filter(user -> "admin".equals(user.getEmails().get(0).getValue()))
            .findFirst()
            .orElseThrow();
    String ownerPath = SCIM_USERS_PATH + "/" + owner.getId();

    // An update that leaves active out is not a deactivation, so it succeeds.
    UserResource unchanged =
        usersApi.updateUser(
            owner.getId(),
            new UserResource().id(owner.getId()).displayName(owner.getDisplayName()));
    assertThat(unchanged.getActive()).isTrue();

    // Each request would leave no enabled metastore owner, so the server refuses it and the owner
    // can still call the API afterwards.
    HttpResponse<String> patch =
        TestUtils.sendRaw(adminConfig, "PATCH", ownerPath, Optional.of(DEACTIVATE));
    assertThat(patch.statusCode()).isEqualTo(400);
    assertThat(patch.body()).contains("\"error_code\":\"FAILED_PRECONDITION\"");
    assertThat(patch.body()).contains(OWNER_REFUSED);
    assertThat(usersApi.getUser(owner.getId()).getActive()).isTrue();

    // The last operation decides: a deactivation last is refused, one followed by an activation is
    // not a deactivation.
    String activate = "{\"op\":\"replace\",\"path\":\"active\",\"value\":true}";
    String deactivate = "{\"op\":\"replace\",\"path\":\"active\",\"value\":false}";
    HttpResponse<String> activateLast = patch(ownerPath, deactivate + "," + activate);
    assertThat(activateLast.statusCode()).isEqualTo(200);
    HttpResponse<String> deactivateLast = patch(ownerPath, activate + "," + deactivate);
    assertThat(deactivateLast.statusCode()).isEqualTo(400);
    assertThat(deactivateLast.body()).contains("\"error_code\":\"FAILED_PRECONDITION\"");
    assertThat(deactivateLast.body()).contains(OWNER_REFUSED);
    assertThat(usersApi.getUser(owner.getId()).getActive()).isTrue();

    UserResource inactive = owner.active(false);
    ApiException put =
        assertThrows(ApiException.class, () -> usersApi.updateUser(owner.getId(), inactive));
    assertThat(put.getCode()).isEqualTo(400);
    assertThat(put.getResponseBody()).contains("\"error_code\":\"FAILED_PRECONDITION\"");
    assertThat(put.getResponseBody()).contains(OWNER_REFUSED);
    assertThat(usersApi.getUser(owner.getId()).getActive()).isTrue();

    ApiException delete =
        assertThrows(ApiException.class, () -> usersApi.deleteUser(owner.getId()));
    assertThat(delete.getCode()).isEqualTo(400);
    assertThat(delete.getResponseBody()).contains("\"error_code\":\"FAILED_PRECONDITION\"");
    assertThat(delete.getResponseBody()).contains(OWNER_REFUSED);
    assertThat(usersApi.getUser(owner.getId()).getActive()).isTrue();
  }

  @Test
  @SneakyThrows
  public void testMetastoreOwnerCanBeDeactivatedAndDeletedWithAuthorizationDisabled() {
    // With authorization disabled the server uses AllowingAuthorizer, which allows every OWNER
    // check, so the service skips the owner check and the owner can be deactivated and deleted.
    String ownerId =
        usersApi.listUsers(null, null, null).getResources().stream()
            .filter(user -> "admin".equals(user.getEmails().get(0).getValue()))
            .findFirst()
            .orElseThrow()
            .getId();
    Properties properties = new Properties();
    properties.putAll(serverProperties);
    properties.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "disable");
    ServerProperties disabled = new ServerProperties(properties);
    Scim2UserService service =
        new Scim2UserService(
            new AllowingAuthorizer(),
            new Repositories(hibernateConfigurator.getSessionFactory(), disabled),
            disabled);

    var owner = service.getUser(ownerId);
    owner.setActive(false);
    assertThat(service.updateUser(ownerId, owner).getActive()).isFalse();
    assertThat(service.deleteUser(ownerId).aggregate().join().status()).isEqualTo(HttpStatus.OK);
  }

  @Test
  @SneakyThrows
  public void testListTotalResultsIsTheMatchCountNotThePageSize() {
    createTestUser("scim-third@localhost", "Third User");
    String filter = URLEncoder.encode("userName sw \"scim-\"", StandardCharsets.UTF_8);

    // {query, expected startIndex, expected Resources, expected itemsPerPage}
    String[][] cases = {
      {"startIndex=1&count=1", "1", "1", "1"},
      {"startIndex=3&count=5", "3", "1", "1"},
      {"startIndex=1&count=50", "1", "3", "3"},
      // A startIndex below 1 is read as 1 (RFC 7644 3.4.2.4).
      {"startIndex=0&count=1", "1", "1", "1"},
      {"startIndex=-5&count=5", "1", "3", "3"},
      // A negative count is read as 0 (RFC 7644 3.4.2.4).
      {"startIndex=1&count=-1", "1", "0", "0"},
    };
    for (String[] c : cases) {
      HttpResponse<String> response =
          TestUtils.sendRaw(
              adminConfig,
              "GET",
              SCIM_USERS_PATH + "?filter=" + filter + "&" + c[0],
              Optional.empty());
      assertThat(response.statusCode()).isEqualTo(200);
      JsonNode body = MAPPER.readTree(response.body());
      assertThat(body.get("totalResults").asInt()).as(c[0]).isEqualTo(3);
      assertThat(body.get("startIndex").asInt()).as(c[0]).isEqualTo(Integer.parseInt(c[1]));
      assertThat(body.path("Resources")).as(c[0]).hasSize(Integer.parseInt(c[2]));
      assertThat(body.get("itemsPerPage").asInt()).as(c[0]).isEqualTo(Integer.parseInt(c[3]));
    }
  }

  @Test
  @SneakyThrows
  public void testListPagesPastTheRepositoryPageSize() {
    // More users than one repository page (PagedListingHelper.DEFAULT_PAGE_SIZE = 100). Display
    // names are distinct because the repository pages on the display name.
    for (int i = 0; i < 105; i++) {
      createTestUser(String.format("scim-page-%03d@localhost", i), String.format("Page %03d", i));
    }
    String filter = URLEncoder.encode("userName sw \"scim-page-\"", StandardCharsets.UTF_8);
    HttpResponse<String> response =
        TestUtils.sendRaw(
            adminConfig,
            "GET",
            SCIM_USERS_PATH + "?filter=" + filter + "&startIndex=101&count=10",
            Optional.empty());
    assertThat(response.statusCode()).isEqualTo(200);
    JsonNode body = MAPPER.readTree(response.body());
    assertThat(body.get("totalResults").asInt()).isEqualTo(105);
    assertThat(body.get("Resources")).hasSize(5);
    assertThat(body.get("Resources").get(0).get("userName").asText())
        .isEqualTo("scim-page-100@localhost");

    // Without count a page holds 50.
    JsonNode defaultPage =
        MAPPER.readTree(
            TestUtils.sendRaw(
                    adminConfig,
                    "GET",
                    SCIM_USERS_PATH + "?filter=" + filter + "&startIndex=1",
                    Optional.empty())
                .body());
    assertThat(defaultPage.get("totalResults").asInt()).isEqualTo(105);
    assertThat(defaultPage.get("Resources")).hasSize(50);
    assertThat(defaultPage.get("itemsPerPage").asInt()).isEqualTo(50);
  }

  @Test
  @SneakyThrows
  public void testPatchHonoursThePathAndAttribute() {
    String activate = "{\"op\":\"replace\",\"path\":\"active\",\"value\":true}";
    String deactivate = "{\"op\":\"replace\",\"path\":\"active\",\"value\":false}";
    // {Operations, expected active afterwards}, each answered 200.
    String[][] applied = {
      {deactivate, "false"},
      {"{\"op\":\"Replace\",\"path\":\"active\",\"value\":true}", "true"},
      {"{\"op\":\"Replace\",\"path\":\"active\",\"value\":\"False\"}", "false"},
      {"{\"op\":\"replace\",\"path\":\"active\",\"value\":\"True\"}", "true"},
      {"{\"op\":\"Replace\",\"value\":{\"active\":false}}", "false"},
      {"{\"op\":\"replace\",\"value\":{\"active\":true}}", "true"},
      {
        "{\"op\":\"replace\",\"path\":\"urn:ietf:params:scim:schemas:core:2.0:User:active\","
            + "\"value\":false}",
        "false"
      },
      // The path, the schema URN and the pathless attribute name ignore case.
      {"{\"op\":\"replace\",\"path\":\"Active\",\"value\":false}", "false"},
      {"{\"op\":\"replace\",\"path\":\"ACTIVE\",\"value\":true}", "true"},
      {
        "{\"op\":\"replace\",\"path\":\"urn:ietf:params:scim:schemas:core:2.0:user:Active\","
            + "\"value\":false}",
        "false"
      },
      {"{\"op\":\"replace\",\"value\":{\"Active\":true}}", "true"},
      // A single-element array is the value itself.
      {"{\"op\":\"replace\",\"path\":\"active\",\"value\":[true]}", "true"},
      {"{\"op\":\"replace\",\"value\":{\"active\":[false]}}", "false"},
      // With several operations the last one wins.
      {activate + "," + deactivate, "false"},
      {deactivate + "," + activate, "true"},
    };
    for (String[] c : applied) {
      assertThat(patchOtherUser(c[0]).statusCode()).as(c[0]).isEqualTo(200);
      assertThat(usersApi.getUser(otherUser.getId()).getActive())
          .as(c[0])
          .isEqualTo(Boolean.parseBoolean(c[1]));
    }

    // Anything but a change of active is not implemented, and applies nothing, even next to an
    // active change. Each case starts from an active user, so an applied deactivation shows.
    String[] notImplemented = {
      "{\"op\":\"replace\",\"value\":{\"displayName\":\"false\"}}",
      "{\"op\":\"replace\",\"path\":\"displayName\",\"value\":\"false\"}",
      "{\"op\":\"replace\",\"value\":{\"active\":false,\"displayName\":\"x\"}}",
      "{\"op\":\"add\",\"path\":\"active\",\"value\":false}",
      "{\"op\":\"replace\",\"path\":\"urn:ietf:params:scim:schemas:extension:enterprise:2.0:User:active\","
          + "\"value\":false}",
      "{\"op\":\"remove\",\"path\":\"active\"}",
      deactivate + ",{\"op\":\"replace\",\"path\":\"displayName\",\"value\":\"x\"}",
      "",
    };
    for (String operations : notImplemented) {
      assertThat(patchOtherUser(activate).statusCode()).isEqualTo(200);
      assertThat(usersApi.getUser(otherUser.getId()).getActive()).isTrue();
      assertThat(patchOtherUser(operations).statusCode()).as(operations).isEqualTo(501);
      assertThat(usersApi.getUser(otherUser.getId()).getActive()).as(operations).isTrue();
    }
  }

  private HttpResponse<String> patchOtherUser(String operations) throws Exception {
    return patch(SCIM_USERS_PATH + "/" + otherUser.getId(), operations);
  }

  private HttpResponse<String> patch(String path, String operations) throws Exception {
    String body =
        "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],\"Operations\":["
            + operations
            + "]}";
    return TestUtils.sendRaw(adminConfig, "PATCH", path, Optional.of(body));
  }

  @Test
  @SneakyThrows
  public void testPatchActiveWithNonBooleanValueIsBadRequest() {
    String maybe = "{\"op\":\"replace\",\"path\":\"active\",\"value\":\"maybe\"}";
    String deactivate = "{\"op\":\"replace\",\"path\":\"active\",\"value\":false}";
    // An array of several values is no boolean either.
    String array = "{\"op\":\"replace\",\"path\":\"active\",\"value\":[false,true]}";
    // Every operation is validated, not only the last, and nothing is applied on a 400.
    for (String operations :
        new String[] {
          maybe,
          array,
          // An empty array is no boolean either.
          "{\"op\":\"replace\",\"path\":\"active\",\"value\":[]}",
          maybe + "," + deactivate,
          deactivate + "," + maybe
        }) {
      HttpResponse<String> response = patchOtherUser(operations);
      assertThat(response.statusCode()).as(operations).isEqualTo(400);
      assertThat(response.body()).as(operations).contains("active must be true or false");
      assertThat(usersApi.getUser(otherUser.getId()).getActive()).as(operations).isTrue();
    }
  }

  @Test
  @SneakyThrows
  public void testPutStoresTheNameInOrderOfPrecedence() {
    // UC stores one name. It is displayName, else name.formatted, else givenName and familyName
    // joined by a space, and the current name is kept when the request carries none. A null
    // expected name means "kept". Okta's rename carries only the name object.
    String[][] cases = {
      {"\"displayName\":\"Shown\",\"name\":{\"formatted\":\"F\",\"givenName\":\"G\"}", "Shown"},
      {
        "\"displayName\":\" \",\"name\":{\"formatted\":\"Formatted\",\"givenName\":\"G\"}",
        "Formatted"
      },
      {
        "\"name\":{\"givenName\":\"Tess\",\"middleName\":\"M\",\"familyName\":\"Renamed\"}",
        "Tess Renamed"
      },
      {"\"name\":{\"givenName\":\"Tess\"}", "Tess"},
      {"\"name\":{\"givenName\":\" \",\"familyName\":\"Renamed\"}", "Renamed"},
      {"\"active\":true", null},
      {"\"displayName\":\"\",\"name\":{\"formatted\":\" \"}", null},
      {"\"name\":{\"givenName\":\"\",\"familyName\":\" \"}", null},
    };
    String path = SCIM_USERS_PATH + "/" + otherUser.getId();
    String current = "Other User";
    for (String[] c : cases) {
      String body = "{\"id\":\"" + otherUser.getId() + "\"," + c[0] + "}";
      HttpResponse<String> put = TestUtils.sendRaw(adminConfig, "PUT", path, Optional.of(body));
      assertThat(put.statusCode()).as(c[0]).isEqualTo(200);
      current = c[1] == null ? current : c[1];
      JsonNode updated = MAPPER.readTree(put.body());
      assertThat(updated.get("displayName").asText()).as(c[0]).isEqualTo(current);
      assertThat(updated.get("name").get("formatted").asText()).as(c[0]).isEqualTo(current);
      JsonNode fetched =
          MAPPER.readTree(TestUtils.sendRaw(adminConfig, "GET", path, Optional.empty()).body());
      assertThat(fetched.get("displayName").asText()).as(c[0]).isEqualTo(current);
    }

    // A name over the 255 characters the column holds is refused, here as the joined 301
    // characters, and the stored name is kept.
    String tooLong =
        "{\"id\":\""
            + otherUser.getId()
            + "\",\"name\":{\"givenName\":\""
            + "g".repeat(200)
            + "\",\"familyName\":\""
            + "f".repeat(100)
            + "\"}}";
    HttpResponse<String> refused =
        TestUtils.sendRaw(adminConfig, "PUT", path, Optional.of(tooLong));
    assertScimBadRequest(refused, "A user name can have at most 255 characters.");
    JsonNode kept =
        MAPPER.readTree(TestUtils.sendRaw(adminConfig, "GET", path, Optional.empty()).body());
    assertThat(kept.get("displayName").asText()).isEqualTo(current);
  }

  @Test
  @SneakyThrows
  public void testPostDerivesTheNameOrRefusesAUserWithoutOne() {
    String[][] created = {
      {
        "scim-post-1@localhost",
        "\"name\":{\"givenName\":\"Tess\",\"familyName\":\"Post\"}",
        "Tess Post"
      },
      {"scim-post-4@localhost", "\"displayName\":\"" + "a".repeat(255) + "\"", "a".repeat(255)},
    };
    for (String[] c : created) {
      HttpResponse<String> post = postUser(c[0], c[1]);
      assertThat(post.statusCode()).as(c[1]).isEqualTo(201);
      JsonNode user = MAPPER.readTree(post.body());
      assertThat(user.get("displayName").asText()).as(c[1]).isEqualTo(c[2]);
      assertThat(user.get("name").get("formatted").asText()).as(c[1]).isEqualTo(c[2]);
    }

    // A name that is missing or over the 255 characters the column holds is the caller's error,
    // and no user is created.
    String[][] refused = {
      {"\"active\":true", "A user needs displayName or name."},
      {
        "\"displayName\":\" \",\"name\":{\"formatted\":\"\",\"givenName\":\" \"}",
        "A user needs displayName or name."
      },
      {
        "\"displayName\":\"" + "a".repeat(256) + "\"",
        "A user name can have at most 255 characters."
      },
    };
    for (String[] c : refused) {
      HttpResponse<String> post = postUser("scim-post-none@localhost", c[0]);
      assertScimBadRequest(post, c[1]);
      String filter =
          URLEncoder.encode("userName eq \"scim-post-none@localhost\"", StandardCharsets.UTF_8);
      JsonNode found =
          MAPPER.readTree(
              TestUtils.sendRaw(
                      adminConfig, "GET", SCIM_USERS_PATH + "?filter=" + filter, Optional.empty())
                  .body());
      assertThat(found.get("totalResults").asInt()).as(c[0]).isZero();
    }
  }

  /** A SCIM 400 for an invalid value, with the message and no SQL or internal type names. */
  @SneakyThrows
  private static void assertScimBadRequest(HttpResponse<String> response, String message) {
    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    JsonNode error = MAPPER.readTree(response.body());
    assertThat(error.get("scimType").asText()).isEqualTo("invalidValue");
    assertThat(error.get("detail").asText()).isEqualTo(message);
    assertThat(response.body())
        .doesNotContain("UserDAO")
        .doesNotContain("io.unitycatalog")
        .doesNotContainIgnoringCase("insert into")
        .doesNotContainIgnoringCase("update uc_");
  }

  private HttpResponse<String> postUser(String email, String fields) throws Exception {
    String body = "{\"emails\":[{\"primary\":true,\"value\":\"" + email + "\"}]," + fields + "}";
    return TestUtils.sendRaw(adminConfig, "POST", SCIM_USERS_PATH, Optional.of(body));
  }

  private static void assertScimPermissionDenied(Executable request) {
    ApiException exception = assertThrows(ApiException.class, request);
    assertThat(exception.getCode()).isEqualTo(403);
    assertThat(exception.getResponseBody()).contains("\"error_code\":\"PERMISSION_DENIED\"");
  }
}
