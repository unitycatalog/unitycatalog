package io.unitycatalog.server.sdk.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.unboundid.scim2.common.exceptions.ServerErrorException;
import io.unitycatalog.control.ApiException;
import io.unitycatalog.control.api.UsersApi;
import io.unitycatalog.control.model.UserResource;
import io.unitycatalog.control.model.UserResourceList;
import io.unitycatalog.server.auth.AllowingAuthorizer;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.BaseExceptionHandler;
import io.unitycatalog.server.exception.Scim2RuntimeException;
import io.unitycatalog.server.exception.ScimExceptionHandler;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.service.Scim2UserService;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.SneakyThrows;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.AfterEach;
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

  private static final String ACTIVATE =
      "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],"
          + "\"Operations\":[{\"op\":\"replace\",\"value\":{\"active\":true}}]}";
  private static final String USER_BODY =
      "{\"schemas\":[\"urn:ietf:params:scim:schemas:core:2.0:User\"],\"displayName\":\"x\"}";
  private static final String SCIM_ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String OWNER_REFUSED =
      "The metastore owner can't be deleted or deactivated: admin";

  private static final String NO_TOKEN = "No authorization found.";

  private final List<String> errorLogs = new CopyOnWriteArrayList<>();
  private final Logger handlerLogger = (Logger) LogManager.getLogger(BaseExceptionHandler.class);
  private final AbstractAppender errorAppender =
      new AbstractAppender(
          "errors", null, null, true, new org.apache.logging.log4j.core.config.Property[0]) {
        @Override
        public void append(LogEvent event) {
          if (event.getLevel().isMoreSpecificThan(Level.ERROR)) {
            errorLogs.add(event.getMessage().getFormattedMessage());
          }
        }
      };

  private UsersApi regularUsersApi;
  private ServerConfig regularUserConfig;
  private UserResource otherUser;

  @BeforeEach
  public void captureErrorLogs() {
    errorAppender.start();
    handlerLogger.addAppender(errorAppender);
  }

  @AfterEach
  public void stopCapturingErrorLogs() {
    handlerLogger.removeAppender(errorAppender);
    errorAppender.stop();
  }

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
    assertScimError(denied.statusCode(), denied.body(), 403, "Access denied.");

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
    assertScimError(patch.statusCode(), patch.body(), 400, OWNER_REFUSED);
    assertThat(usersApi.getUser(owner.getId()).getActive()).isTrue();

    UserResource inactive = owner.active(false);
    ApiException put =
        assertThrows(ApiException.class, () -> usersApi.updateUser(owner.getId(), inactive));
    assertScimError(put.getCode(), put.getResponseBody(), 400, OWNER_REFUSED);
    assertThat(usersApi.getUser(owner.getId()).getActive()).isTrue();

    ApiException delete =
        assertThrows(ApiException.class, () -> usersApi.deleteUser(owner.getId()));
    assertScimError(delete.getCode(), delete.getResponseBody(), 400, OWNER_REFUSED);
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
  public void testErrorsAreScimErrors() {
    String notUuid = "not-a-uuid";
    // method, body: each method on an id that is not a UUID is a SCIM 404
    List<Object[]> cases =
        List.of(
            new Object[] {"PUT", USER_BODY},
            new Object[] {"DELETE", null},
            new Object[] {"PATCH", DEACTIVATE},
            new Object[] {"PATCH", ACTIVATE});
    for (Object[] c : cases) {
      HttpResponse<String> response =
          TestUtils.sendRaw(
              adminConfig,
              (String) c[0],
              SCIM_USERS_PATH + "/" + notUuid,
              Optional.ofNullable((String) c[1]));
      assertScimError(response, 404, "User not found: " + notUuid);
    }
  }

  /**
   * An IdP provisioning a user, then hitting each error it can meet, as it sees them on the wire.
   */
  @Test
  @SneakyThrows
  public void testScimErrorJourney() {
    String email = "test.user@okta.local";
    String oktaUser =
        "{\"schemas\":[\"urn:ietf:params:scim:schemas:core:2.0:User\"],"
            + "\"userName\":\""
            + email
            + "\",\"name\":{\"givenName\":\"Test\",\"familyName\":\"User\"},"
            + "\"emails\":[{\"primary\":true,\"value\":\""
            + email
            + "\",\"type\":\"work\"}],\"displayName\":\"Test User\","
            + "\"locale\":\"en-US\",\"externalId\":\"00ujl29u0le5T6Aj10h7\","
            + "\"groups\":[],\"password\":\"1mz050nq\",\"active\":true}";
    String unknownId = UUID.randomUUID().toString();

    // Provision the user.
    HttpResponse<String> created =
        TestUtils.sendRaw(adminConfig, "POST", SCIM_USERS_PATH, Optional.of(oktaUser));
    assertThat(created.statusCode()).as(created.body()).isEqualTo(201);
    assertScimContentType(created);

    // Provisioning the same user again is a uniqueness conflict naming the email.
    HttpResponse<String> duplicate =
        TestUtils.sendRaw(adminConfig, "POST", SCIM_USERS_PATH, Optional.of(oktaUser));
    assertScimError(duplicate, 409, "User already exists: " + email);
    assertThat(MAPPER.readTree(duplicate.body()).path("scimType").asText()).isEqualTo("uniqueness");

    // Giving another user this user's external id is the same kind of conflict.
    String otherId = otherUser.getId();
    String takeExternalId =
        "{\"schemas\":[\"urn:ietf:params:scim:schemas:core:2.0:User\"],\"id\":\""
            + otherId
            + "\",\"displayName\":\"Other User\",\"externalId\":\"00ujl29u0le5T6Aj10h7\"}";
    HttpResponse<String> externalIdTaken =
        TestUtils.sendRaw(
            adminConfig, "PUT", SCIM_USERS_PATH + "/" + otherId, Optional.of(takeExternalId));
    assertScimError(externalIdTaken, 409, "User already exists with external id");
    assertThat(MAPPER.readTree(externalIdTaken.body()).path("scimType").asText())
        .isEqualTo("uniqueness");

    // Reading users that do not exist: an unknown id and an id that is not even a UUID.
    assertScimError(
        TestUtils.sendRaw(adminConfig, "GET", SCIM_USERS_PATH + "/" + unknownId, Optional.empty()),
        404,
        "User not found: " + unknownId);
    assertScimError(
        TestUtils.sendRaw(adminConfig, "GET", SCIM_USERS_PATH + "/not-a-uuid", Optional.empty()),
        404,
        "User not found: not-a-uuid");

    // An operation the server does not support is a 501, not logged as a server failure.
    String createdId = MAPPER.readTree(created.body()).get("id").asText();
    String profileUpdate =
        "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],"
            + "\"Operations\":[{\"op\":\"add\",\"path\":\"name.givenName\","
            + "\"value\":\"Renamed\"}]}";
    assertScimError(
        TestUtils.sendRaw(
            adminConfig, "PATCH", SCIM_USERS_PATH + "/" + createdId, Optional.of(profileUpdate)),
        501,
        "PATCH supports only");
    assertThat(errorLogs).as("a client-caused 501 is not logged as an error").isEmpty();

    // A request without a token is a 401.
    assertScimError(sendWithoutToken("GET", SCIM_USERS_PATH, Optional.empty()), 401, NO_TOKEN);
  }

  @Test
  public void testScimServerErrorIsLogged() {
    HttpRequest req = HttpRequest.of(HttpMethod.POST, SCIM_USERS_PATH);
    AggregatedHttpResponse response =
        ScimExceptionHandler.INSTANCE
            .handleException(
                ServiceRequestContext.of(req),
                req,
                new Scim2RuntimeException(
                    new ServerErrorException("db down", null, new RuntimeException("cause"))))
            .aggregate()
            .join();
    assertThat(response.status()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(errorLogs).as("a SCIM server failure is logged as an error").isNotEmpty();
  }

  private HttpResponse<String> sendWithoutToken(String method, String path, Optional<String> body)
      throws Exception {
    return TestUtils.sendRaw(new ServerConfig(adminConfig.getServerUrl(), ""), method, path, body);
  }

  private static void assertScimContentType(HttpResponse<String> response) {
    assertThat(response.headers().firstValue("content-type").orElse(""))
        .startsWith("application/scim+json");
  }

  private static void assertScimError(HttpResponse<String> response, int status, String detail)
      throws Exception {
    assertScimError(response.statusCode(), response.body(), status, detail);
    assertScimContentType(response);
  }

  private static void assertScimError(int actualStatus, String body, int status, String detail)
      throws Exception {
    String description = actualStatus + " " + body;
    assertThat(actualStatus).as(description).isEqualTo(status);
    JsonNode error = MAPPER.readTree(body);
    assertThat(error.get("schemas")).as(description).hasSize(1);
    assertThat(error.get("schemas").get(0).asText()).as(description).isEqualTo(SCIM_ERROR_SCHEMA);
    assertThat(error.get("status").asText()).as(description).isEqualTo(String.valueOf(status));
    assertThat(error.get("detail").asText()).as(description).contains(detail);
    error.forEach(field -> assertThat(field.isNull()).as(description).isFalse());
  }

  private static void assertScimPermissionDenied(Executable request) throws Exception {
    ApiException exception = assertThrows(ApiException.class, request);
    assertScimError(exception.getCode(), exception.getResponseBody(), 403, "Access denied.");
  }
}
