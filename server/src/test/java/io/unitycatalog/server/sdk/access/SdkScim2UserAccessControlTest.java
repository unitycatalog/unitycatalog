package io.unitycatalog.server.sdk.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.linecorp.armeria.common.HttpStatus;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.control.ApiException;
import io.unitycatalog.control.api.UsersApi;
import io.unitycatalog.control.model.Email;
import io.unitycatalog.control.model.UserResource;
import io.unitycatalog.control.model.UserResourceList;
import io.unitycatalog.server.auth.AllowingAuthorizer;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.service.Scim2UserService;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.util.List;
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
  public void testDeleteRemovesTheUserAndFreesTheEmail() {
    grantPermissions(
        OTHER_USER, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    String catalogPath = "/api/2.1/unity-catalog/catalogs/" + TestUtils.CATALOG_NAME;
    assertThat(
            TestUtils.sendRawGet(createTestUserServerConfig(OTHER_USER), catalogPath).statusCode())
        .isEqualTo(200);
    String userPath = SCIM_USERS_PATH + "/" + otherUser.getId();
    String put =
        "{\"schemas\":[\"urn:ietf:params:scim:schemas:core:2.0:User\"],\"id\":\""
            + otherUser.getId()
            + "\",\"displayName\":\"Renamed\"}";

    assertThat(TestUtils.sendRaw(adminConfig, "DELETE", userPath, Optional.empty()).statusCode())
        .isEqualTo(200);

    // Every later call on the id is a 404.
    String notFound = "User not found: " + otherUser.getId();
    for (HttpResponse<String> response :
        List.of(
            TestUtils.sendRaw(adminConfig, "GET", userPath, Optional.empty()),
            TestUtils.sendRaw(adminConfig, "PUT", userPath, Optional.of(put)),
            TestUtils.sendRaw(adminConfig, "PATCH", userPath, Optional.of(DEACTIVATE)),
            TestUtils.sendRaw(adminConfig, "DELETE", userPath, Optional.empty()))) {
      assertThat(response.statusCode()).isEqualTo(404);
      assertThat(response.body()).contains(notFound);
    }
    assertThat(usersApi.listUsers(null, null, null).getResources())
        .extracting(UserResource::getId)
        .doesNotContain(otherUser.getId());

    // The email can be created again, as a new user without the old grants.
    UserResource recreated = createTestUser(OTHER_USER, "Other User Again");
    assertThat(recreated.getId()).isNotEqualTo(otherUser.getId());
    assertThat(
            TestUtils.sendRawGet(createTestUserServerConfig(OTHER_USER), catalogPath).statusCode())
        .isEqualTo(403);
  }

  @Test
  @SneakyThrows
  public void testPutUpdatesTheEmail() {
    UserResource renamed =
        usersApi.updateUser(
            otherUser.getId(),
            new UserResource()
                .id(otherUser.getId())
                .emails(List.of(primaryEmail("moved@localhost"))));

    assertThat(renamed.getId()).isEqualTo(otherUser.getId());
    assertThat(renamed.getEmails().get(0).getValue()).isEqualTo("moved@localhost");
    assertThat(usersApi.getUser(otherUser.getId()).getEmails().get(0).getValue())
        .isEqualTo("moved@localhost");
    // The old email is free again, and there is no alias for it.
    createTestUser(OTHER_USER, "Takes The Old Email");

    // The user's own email is not a clash, and a list without a primary email is refused as on
    // create.
    assertThat(
            usersApi
                .updateUser(
                    otherUser.getId(),
                    new UserResource()
                        .id(otherUser.getId())
                        .emails(List.of(primaryEmail("moved@localhost"))))
                .getEmails()
                .get(0)
                .getValue())
        .isEqualTo("moved@localhost");
    ApiException noPrimary =
        assertThrows(
            ApiException.class,
            () ->
                usersApi.updateUser(
                    otherUser.getId(),
                    new UserResource()
                        .id(otherUser.getId())
                        .emails(List.of(new Email().value("other@localhost").primary(false)))));
    assertThat(noPrimary.getCode()).isEqualTo(412);
    assertThat(noPrimary.getResponseBody()).contains("User does not have a primary email.");

    // An email that belongs to another user is a conflict.
    ApiException clash =
        assertThrows(
            ApiException.class,
            () ->
                usersApi.updateUser(
                    otherUser.getId(),
                    new UserResource()
                        .id(otherUser.getId())
                        .emails(List.of(primaryEmail(REGULAR_USER)))));
    assertThat(clash.getCode()).isEqualTo(409);
    assertThat(clash.getResponseBody()).contains("User already exists: " + REGULAR_USER);
    assertThat(usersApi.getUser(otherUser.getId()).getEmails().get(0).getValue())
        .isEqualTo("moved@localhost");
  }

  @Test
  @SneakyThrows
  public void testPutWithAbsentOrEmptyEmailsKeepsTheEmail() {
    // `uc user update` without --email sends `emails: []`.
    for (List<Email> emails : java.util.Arrays.asList(null, List.<Email>of())) {
      UserResource updated =
          usersApi.updateUser(
              otherUser.getId(),
              new UserResource().id(otherUser.getId()).displayName("Renamed").emails(emails));

      assertThat(updated.getDisplayName()).isEqualTo("Renamed");
      assertThat(updated.getEmails().get(0).getValue()).isEqualTo(OTHER_USER);
      assertThat(usersApi.getUser(otherUser.getId()).getEmails().get(0).getValue())
          .isEqualTo(OTHER_USER);
    }
  }

  private static Email primaryEmail(String value) {
    return new Email().value(value).primary(true);
  }

  private static void assertScimPermissionDenied(Executable request) {
    ApiException exception = assertThrows(ApiException.class, request);
    assertThat(exception.getCode()).isEqualTo(403);
    assertThat(exception.getResponseBody()).contains("\"error_code\":\"PERMISSION_DENIED\"");
  }
}
