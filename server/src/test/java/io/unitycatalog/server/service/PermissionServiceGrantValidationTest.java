package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linecorp.armeria.common.HttpResponse;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.PermissionsChange;
import io.unitycatalog.server.model.PermissionsList;
import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.PrivilegeAssignment;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.UpdatePermissions;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.CredentialRepository;
import io.unitycatalog.server.persist.ExternalLocationRepository;
import io.unitycatalog.server.persist.FunctionRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.ModelRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.VolumeRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.JsonUtils;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;

public class PermissionServiceGrantValidationTest {
  private static final String NAME = "main.default.target";
  private static final String ALICE = "alice@example.com";
  private static final String BOB = "bob@example.com";
  private static final String UNKNOWN = "typo@example.com";
  private static final String GRANT_ALICE =
      "{'principal': 'alice@example.com', 'add': ['READ METADATA'], 'remove': []}";

  private final UnityCatalogAuthorizer authorizer = mock();
  private final UUID resourceId = UUID.randomUUID();
  private final UUID aliceId = UUID.randomUUID();
  private final UUID bobId = UUID.randomUUID();
  private final UUID carolId = UUID.randomUUID();
  private PermissionService service;

  @BeforeEach
  public void setUp() {
    Repositories repositories = mock();
    UserRepository users = mock();
    MetastoreRepository metastores = mock();
    CatalogRepository catalogs = mock();
    SchemaRepository schemas = mock();
    TableRepository tables = mock();
    FunctionRepository functions = mock();
    VolumeRepository volumes = mock();
    ModelRepository models = mock();
    ExternalLocationRepository externalLocations = mock();
    CredentialRepository credentials = mock();
    when(repositories.getUserRepository()).thenReturn(users);
    when(repositories.getMetastoreRepository()).thenReturn(metastores);
    when(repositories.getCatalogRepository()).thenReturn(catalogs);
    when(repositories.getSchemaRepository()).thenReturn(schemas);
    when(repositories.getTableRepository()).thenReturn(tables);
    when(repositories.getFunctionRepository()).thenReturn(functions);
    when(repositories.getVolumeRepository()).thenReturn(volumes);
    when(repositories.getModelRepository()).thenReturn(models);
    when(repositories.getExternalLocationRepository()).thenReturn(externalLocations);
    when(repositories.getCredentialRepository()).thenReturn(credentials);
    String id = resourceId.toString();
    when(metastores.getMetastoreId()).thenReturn(resourceId);
    when(catalogs.getCatalog(NAME)).thenReturn(new CatalogInfo().id(id));
    when(schemas.getSchema(NAME)).thenReturn(new SchemaInfo().schemaId(id));
    when(tables.getTable(NAME)).thenReturn(new TableInfo().tableId(id));
    when(functions.getFunction(NAME)).thenReturn(new FunctionInfo().functionId(id));
    when(volumes.getVolume(NAME)).thenReturn(new VolumeInfo().volumeId(id));
    when(models.getRegisteredModel(NAME)).thenReturn(new RegisteredModelInfo().id(id));
    when(externalLocations.getExternalLocation(NAME)).thenReturn(new ExternalLocationInfo().id(id));
    when(credentials.getCredential(NAME)).thenReturn(new CredentialInfo().id(id));
    when(users.getUserByEmail(ALICE)).thenReturn(new User().id(aliceId.toString()));
    when(users.getUserByEmail(BOB)).thenReturn(new User().id(bobId.toString()));
    when(users.getUserByEmail(UNKNOWN))
        .thenThrow(new BaseException(ErrorCode.NOT_FOUND, "User not found: " + UNKNOWN));
    when(users.getUser(aliceId.toString())).thenReturn(new User().email(ALICE));
    when(users.getUser(bobId.toString())).thenReturn(new User().email(BOB));
    service = new PermissionService(authorizer, repositories, mock(ServerProperties.class));
  }

  /** Reads {@code json}, written with single quotes, as the server reads a PATCH body. */
  private HttpResponse update(SecurableType type, String json) throws Exception {
    UpdatePermissions request =
        JsonUtils.getInstance().readValue(json.replace('\'', '"'), UpdatePermissions.class);
    return switch (type) {
      case METASTORE -> service.updateMetastoreAuthorization(NAME, request);
      case CATALOG -> service.updateCatalogAuthorization(NAME, request);
      case SCHEMA -> service.updateSchemaAuthorization(NAME, request);
      case TABLE -> service.updateTableAuthorization(NAME, request);
      case FUNCTION -> service.updateFunctionAuthorization(NAME, request);
      case VOLUME -> service.updateVolumeAuthorization(NAME, request);
      case REGISTERED_MODEL -> service.updateRegisteredModelAuthorization(NAME, request);
      case EXTERNAL_LOCATION -> service.updateExternalLocationAuthorization(NAME, request);
      case CREDENTIAL -> service.updateCredentialAuthorization(NAME, request);
    };
  }

  private static Stream<Arguments> rejectedBodies() {
    String addNull = "add must be a list of privileges, without null.";
    String removeNull = "remove must be a list of privileges, without null.";
    String noPrincipal = "principal is required in each change.";
    List<List<Object>> bodies =
        List.of(
            List.of("{'changes': null}", ErrorCode.INVALID_ARGUMENT, "changes is required."),
            List.of(
                "{'changes': [" + GRANT_ALICE + ", null]}",
                ErrorCode.INVALID_ARGUMENT,
                "changes must not contain null."),
            List.of(
                "{'changes': [" + GRANT_ALICE + ", {'principal': null}]}",
                ErrorCode.INVALID_ARGUMENT,
                noPrincipal),
            List.of(
                "{'changes': [" + GRANT_ALICE + ", {'add': ['READ METADATA']}]}",
                ErrorCode.INVALID_ARGUMENT,
                noPrincipal),
            List.of(
                "{'changes': [" + GRANT_ALICE + ", {'principal': 'bob@example.com', 'add': null}]}",
                ErrorCode.INVALID_ARGUMENT,
                addNull),
            List.of(
                "{'changes': ["
                    + GRANT_ALICE
                    + ", {'principal': 'bob@example.com', 'add': ['READ METADATA', null]}]}",
                ErrorCode.INVALID_ARGUMENT,
                addNull),
            List.of(
                "{'changes': [{'principal': 'alice@example.com', 'add': ['READ METADATA'],"
                    + " 'remove': null}]}",
                ErrorCode.INVALID_ARGUMENT,
                removeNull),
            List.of(
                "{'changes': [{'principal': 'alice@example.com', 'add': ['READ METADATA'],"
                    + " 'remove': [null]}]}",
                ErrorCode.INVALID_ARGUMENT,
                removeNull),
            List.of(
                "{'changes': ["
                    + GRANT_ALICE
                    + ", {'principal': 'typo@example.com', 'add': ['READ METADATA']}]}",
                ErrorCode.NOT_FOUND,
                "User not found: typo@example.com"));
    return Arrays.stream(SecurableType.values())
        .flatMap(
            type ->
                bodies.stream()
                    .map(body -> Arguments.of(type, body.get(0), body.get(1), body.get(2))));
  }

  @ParameterizedTest
  @MethodSource("rejectedBodies")
  public void rejectedUpdateWritesNoGrant(
      SecurableType type, String json, ErrorCode errorCode, String message) {
    Throwable thrown = catchThrowable(() -> update(type, json));

    verify(authorizer, never()).grantAuthorization(any(), any(), any());
    verify(authorizer, never()).revokeAuthorization(any(), any(), any());
    assertThat(thrown)
        .isInstanceOfSatisfying(
            BaseException.class,
            exception -> assertThat(exception.getErrorCode()).isEqualTo(errorCode))
        .hasMessage(message);
  }

  @ParameterizedTest
  @EnumSource(SecurableType.class)
  public void updateAppliesEveryChangeInOrder(SecurableType type) throws Exception {
    when(authorizer.listAuthorizations(resourceId))
        .thenReturn(
            Map.of(
                aliceId,
                List.of(Privileges.READ_METADATA),
                bobId,
                List.of(Privileges.MODIFY),
                carolId,
                List.of(Privileges.SELECT)));

    HttpResponse response =
        update(
            type,
            "{'changes': [{'principal': 'alice@example.com', 'add': ['READ METADATA']},"
                + " {'principal': 'bob@example.com', 'remove': ['SELECT']},"
                + " {'principal': 'alice@example.com', 'remove': ['MODIFY']}]}");

    InOrder order = inOrder(authorizer);
    order.verify(authorizer).grantAuthorization(aliceId, resourceId, Privileges.READ_METADATA);
    order.verify(authorizer).revokeAuthorization(bobId, resourceId, Privileges.SELECT);
    order.verify(authorizer).revokeAuthorization(aliceId, resourceId, Privileges.MODIFY);
    PermissionsList permissions =
        JsonUtils.getInstance()
            .readValue(response.aggregate().join().contentUtf8(), PermissionsList.class);
    assertThat(permissions.getPrivilegeAssignments())
        .containsExactlyInAnyOrder(
            new PrivilegeAssignment().principal(ALICE).privileges(List.of(Privilege.READ_METADATA)),
            new PrivilegeAssignment().principal(BOB).privileges(List.of(Privilege.MODIFY)));
  }

  @ParameterizedTest
  @EnumSource(SecurableType.class)
  public void additionsRequireAValidObjectType(SecurableType type) {
    Set<Privilege> validPrivileges =
        switch (type) {
          case METASTORE ->
              Set.of(
                  Privilege.CREATE_CATALOG,
                  Privilege.CREATE_STORAGE_CREDENTIAL,
                  Privilege.CREATE_EXTERNAL_LOCATION);
          case CATALOG ->
              Set.of(
                  Privilege.BROWSE,
                  Privilege.MANAGE,
                  Privilege.USE_CATALOG,
                  Privilege.CREATE_SCHEMA,
                  Privilege.USE_SCHEMA,
                  Privilege.CREATE_TABLE,
                  Privilege.SELECT,
                  Privilege.MODIFY,
                  Privilege.CREATE_FUNCTION,
                  Privilege.EXECUTE,
                  Privilege.CREATE_VOLUME,
                  Privilege.READ_VOLUME,
                  Privilege.WRITE_VOLUME,
                  Privilege.CREATE_MODEL,
                  Privilege.CREATE_MODEL_VERSION);
          case SCHEMA ->
              Set.of(
                  Privilege.MANAGE,
                  Privilege.USE_SCHEMA,
                  Privilege.CREATE_TABLE,
                  Privilege.SELECT,
                  Privilege.MODIFY,
                  Privilege.CREATE_FUNCTION,
                  Privilege.EXECUTE,
                  Privilege.CREATE_VOLUME,
                  Privilege.READ_VOLUME,
                  Privilege.WRITE_VOLUME,
                  Privilege.CREATE_MODEL,
                  Privilege.CREATE_MODEL_VERSION);
          case TABLE -> Set.of(Privilege.SELECT, Privilege.MODIFY, Privilege.MANAGE);
          case FUNCTION -> Set.of(Privilege.EXECUTE, Privilege.MANAGE);
          case REGISTERED_MODEL ->
              Set.of(Privilege.EXECUTE, Privilege.CREATE_MODEL_VERSION, Privilege.MANAGE);
          case VOLUME -> Set.of(Privilege.READ_VOLUME, Privilege.WRITE_VOLUME, Privilege.MANAGE);
          case EXTERNAL_LOCATION ->
              Set.of(
                  Privilege.BROWSE,
                  Privilege.MANAGE,
                  Privilege.READ_FILES,
                  Privilege.WRITE_FILES,
                  Privilege.CREATE_EXTERNAL_TABLE,
                  Privilege.CREATE_EXTERNAL_VOLUME,
                  Privilege.CREATE_MANAGED_STORAGE);
          case CREDENTIAL ->
              Set.of(
                  Privilege.MANAGE,
                  Privilege.CREATE_EXTERNAL_LOCATION,
                  Privilege.READ_FILES,
                  Privilege.WRITE_FILES,
                  Privilege.CREATE_EXTERNAL_TABLE);
        };

    for (Privilege privilege : Privilege.values()) {
      List<PermissionsChange> changes =
          List.of(new PermissionsChange().principal("reader").add(List.of(privilege)));
      if (privilege == Privilege.READ_METADATA || validPrivileges.contains(privilege)) {
        assertThatCode(() -> PermissionService.validateAddedPrivileges(type, changes))
            .doesNotThrowAnyException();
      } else {
        assertThatThrownBy(() -> PermissionService.validateAddedPrivileges(type, changes))
            .isInstanceOfSatisfying(
                BaseException.class,
                exception ->
                    assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT))
            .hasMessage(
                "Privilege " + privilege.getValue() + " cannot be granted on " + type.getValue());
      }

      List<PermissionsChange> removals =
          List.of(new PermissionsChange().principal("reader").remove(List.of(privilege)));
      assertThatCode(() -> PermissionService.validateAddedPrivileges(type, removals))
          .doesNotThrowAnyException();
    }
  }
}
