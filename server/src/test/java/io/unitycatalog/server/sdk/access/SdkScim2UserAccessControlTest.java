package io.unitycatalog.server.sdk.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.control.ApiException;
import io.unitycatalog.control.api.UsersApi;
import io.unitycatalog.control.model.UserResource;
import io.unitycatalog.control.model.UserResourceList;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class SdkScim2UserAccessControlTest extends SdkAccessControlBaseCRUDTest {
  private static final String REGULAR_USER = "scim-regular@localhost";
  private static final String OTHER_USER = "scim-other@localhost";
  private static final String SCIM_USERS_PATH = "/api/1.0/unity-control/scim2/Users";

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
    String deactivate =
        "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],"
            + "\"Operations\":[{\"op\":\"replace\",\"value\":{\"active\":false}}]}";

    // A regular user cannot patch another user.
    HttpResponse<String> denied =
        TestUtils.sendRaw(regularUserConfig, "PATCH", patchPath, Optional.of(deactivate));
    assertThat(denied.statusCode()).isEqualTo(403);
    assertThat(denied.body()).contains("\"error_code\":\"PERMISSION_DENIED\"");

    // The metastore admin can, and the user ends up deactivated.
    HttpResponse<String> allowed =
        TestUtils.sendRaw(adminConfig, "PATCH", patchPath, Optional.of(deactivate));
    assertThat(allowed.statusCode()).isEqualTo(200);
    assertThat(usersApi.getUser(otherUser.getId()).getActive()).isFalse();
  }

  private static void assertScimPermissionDenied(Executable request) {
    ApiException exception = assertThrows(ApiException.class, request);
    assertThat(exception.getCode()).isEqualTo(403);
    assertThat(exception.getResponseBody()).contains("\"error_code\":\"PERMISSION_DENIED\"");
  }
}
