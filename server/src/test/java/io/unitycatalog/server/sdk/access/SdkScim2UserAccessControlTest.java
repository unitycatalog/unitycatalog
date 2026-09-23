package io.unitycatalog.server.sdk.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.control.ApiException;
import io.unitycatalog.control.api.UsersApi;
import io.unitycatalog.control.model.UserResource;
import io.unitycatalog.control.model.UserResourceList;
import io.unitycatalog.server.base.ServerConfig;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class SdkScim2UserAccessControlTest extends SdkAccessControlBaseCRUDTest {
  private static final String REGULAR_USER = "scim-regular@localhost";
  private static final String OTHER_USER = "scim-other@localhost";

  private UsersApi regularUsersApi;
  private UserResource otherUser;

  @BeforeEach
  public void setUpUsers() throws Exception {
    createTestUser(REGULAR_USER, "Regular User");
    otherUser = createTestUser(OTHER_USER, "Other User");

    ServerConfig regularUserConfig = createTestUserServerConfig(REGULAR_USER);
    regularUsersApi = new UsersApi(createControlApiClient(regularUserConfig));
  }

  @Test
  @SneakyThrows
  public void testOnlyMetastoreAdminCanListAndGetUsers() {
    UserResourceList users = usersApi.listUsers(null, null, null);
    assertThat(users.getResources()).extracting(UserResource::getId).contains(otherUser.getId());

    UserResource fetchedUser = usersApi.getUser(otherUser.getId());
    assertThat(fetchedUser.getId()).isEqualTo(otherUser.getId());
    assertThat(fetchedUser.getEmails().get(0).getValue()).isEqualTo(OTHER_USER);

    assertScimPermissionDenied(() -> regularUsersApi.listUsers(null, null, null));
    assertScimPermissionDenied(() -> regularUsersApi.getUser(otherUser.getId()));
  }

  private static void assertScimPermissionDenied(Executable request) {
    ApiException exception = assertThrows(ApiException.class, request);
    assertThat(exception.getCode()).isEqualTo(403);
    assertThat(exception.getResponseBody()).contains("\"error_code\":\"PERMISSION_DENIED\"");
  }
}
