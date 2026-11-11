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
  }

  @Test
  @SneakyThrows
  public void testPatchHonoursThePathAndAttribute() {
    String patchPath = SCIM_USERS_PATH + "/" + otherUser.getId();
    // {Operations element, expected status, expected active afterwards or "unchanged"}. A refused
    // request leaves the previous state alone.
    String[][] cases = {
      {"{\"op\":\"replace\",\"path\":\"active\",\"value\":false}", "200", "false"},
      {"{\"op\":\"Replace\",\"path\":\"active\",\"value\":true}", "200", "true"},
      {"{\"op\":\"Replace\",\"path\":\"active\",\"value\":\"False\"}", "200", "false"},
      {"{\"op\":\"replace\",\"path\":\"active\",\"value\":\"True\"}", "200", "true"},
      {"{\"op\":\"Replace\",\"value\":{\"active\":false}}", "200", "false"},
      {"{\"op\":\"replace\",\"value\":{\"active\":true}}", "200", "true"},
      {
        "{\"op\":\"replace\",\"path\":\"urn:ietf:params:scim:schemas:core:2.0:User:active\","
            + "\"value\":false}",
        "200",
        "false"
      },
      // Anything but a change of active is not implemented, and must not touch active.
      {"{\"op\":\"replace\",\"value\":{\"displayName\":\"false\"}}", "501", "unchanged"},
      {"{\"op\":\"replace\",\"path\":\"displayName\",\"value\":\"false\"}", "501", "unchanged"},
      {
        "{\"op\":\"replace\",\"value\":{\"active\":false,\"displayName\":\"x\"}}",
        "501",
        "unchanged"
      },
      {"{\"op\":\"add\",\"path\":\"active\",\"value\":false}", "501", "unchanged"},
      {
        "{\"op\":\"replace\",\"path\":\"urn:ietf:params:scim:schemas:extension:enterprise:2.0:User:active\","
            + "\"value\":false}",
        "501",
        "unchanged"
      },
      {"{\"op\":\"remove\",\"path\":\"active\"}", "501", "unchanged"},
    };
    for (String[] c : cases) {
      String body =
          "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],\"Operations\":["
              + c[0]
              + "]}";
      boolean activeBefore = usersApi.getUser(otherUser.getId()).getActive();
      HttpResponse<String> response =
          TestUtils.sendRaw(adminConfig, "PATCH", patchPath, Optional.of(body));
      assertThat(response.statusCode()).as(c[0]).isEqualTo(Integer.parseInt(c[1]));
      boolean expectedActive = c[1].equals("200") ? Boolean.parseBoolean(c[2]) : activeBefore;
      assertThat(usersApi.getUser(otherUser.getId()).getActive())
          .as(c[0])
          .isEqualTo(expectedActive);
    }
  }

  @Test
  @SneakyThrows
  public void testPatchActiveWithNonBooleanValueIsBadRequest() {
    String maybe = "{\"op\":\"replace\",\"path\":\"active\",\"value\":\"maybe\"}";
    String deactivate = "{\"op\":\"replace\",\"path\":\"active\",\"value\":false}";
    // Every operation is validated, not only the last, and nothing is applied on a 400.
    for (String operations :
        new String[] {maybe, maybe + "," + deactivate, deactivate + "," + maybe}) {
      String body =
          "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],\"Operations\":["
              + operations
              + "]}";
      HttpResponse<String> response =
          TestUtils.sendRaw(
              adminConfig, "PATCH", SCIM_USERS_PATH + "/" + otherUser.getId(), Optional.of(body));
      assertThat(response.statusCode()).as(operations).isEqualTo(400);
      assertThat(response.body()).as(operations).contains("active must be true or false");
      assertThat(usersApi.getUser(otherUser.getId()).getActive()).as(operations).isTrue();
    }
  }

  @Test
  @SneakyThrows
  public void testUserCarriesTheNameAttribute() {
    HttpResponse<String> response =
        TestUtils.sendRaw(
            adminConfig, "GET", SCIM_USERS_PATH + "/" + otherUser.getId(), Optional.empty());
    assertThat(response.statusCode()).isEqualTo(200);
    JsonNode body = MAPPER.readTree(response.body());
    assertThat(body.get("name").get("formatted").asText()).isEqualTo("Other User");
    // userName stays the email (plan Q-P1).
    assertThat(body.get("userName").asText()).isEqualTo(OTHER_USER);
  }

  private static void assertScimPermissionDenied(Executable request) {
    ApiException exception = assertThrows(ApiException.class, request);
    assertThat(exception.getCode()).isEqualTo(403);
    assertThat(exception.getResponseBody()).contains("\"error_code\":\"PERMISSION_DENIED\"");
  }
}
