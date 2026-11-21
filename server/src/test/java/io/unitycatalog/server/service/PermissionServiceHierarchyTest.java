package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.CREDENTIAL;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.FUNCTION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.model.SecurableType.VOLUME;
import static io.unitycatalog.server.persist.model.Privileges.MODIFY;
import static io.unitycatalog.server.persist.model.Privileges.OWNER;
import static io.unitycatalog.server.persist.model.Privileges.SELECT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Server;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.PermissionsList;
import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.PrivilegeAssignment;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
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
import io.unitycatalog.server.utils.JsonUtils;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

public class PermissionServiceHierarchyTest {
  private static final String ALICE = "alice@example.com";
  private static final String BOB = "bob@example.com";
  private static final PrivilegeAssignment ALICE_SELECT =
      new PrivilegeAssignment().principal(ALICE).privileges(List.of(Privilege.SELECT));
  private static final PrivilegeAssignment BOB_MODIFY =
      new PrivilegeAssignment().principal(BOB).privileges(List.of(Privilege.MODIFY));
  private static final Map<String, PrivilegeAssignment> ASSIGNMENTS_BY_EMAIL =
      Map.of(ALICE, ALICE_SELECT, BOB, BOB_MODIFY);

  private final UnityCatalogAuthorizer authorizer = mock();
  private final UserRepository users = mock();
  private final UUID principalId = UUID.randomUUID();
  private final UUID otherPrincipalId = UUID.randomUUID();
  private final Map<SecurableType, UUID> resources =
      Arrays.stream(SecurableType.values())
          .collect(Collectors.toMap(type -> type, type -> UUID.randomUUID()));
  private final Map<SecurableType, String> names =
      Map.of(
          METASTORE, "metastore",
          CATALOG, "analytics",
          SCHEMA, "analytics.reports",
          TABLE, "analytics.reports.orders",
          FUNCTION, "analytics.reports.total",
          VOLUME, "analytics.reports.files",
          REGISTERED_MODEL, "analytics.reports.forecast",
          EXTERNAL_LOCATION, "landing",
          CREDENTIAL, "landing_role");
  private PermissionService service;

  @BeforeEach
  public void setUp() {
    Repositories repositories = mock();
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
    when(users.findPrincipalId()).thenReturn(principalId);
    when(users.getUser(principalId.toString())).thenReturn(new User().email(ALICE));
    when(users.getUser(otherPrincipalId.toString())).thenReturn(new User().email(BOB));
    when(metastores.getMetastoreId()).thenReturn(resources.get(METASTORE));
    when(catalogs.getCatalog(names.get(CATALOG))).thenReturn(new CatalogInfo().id(id(CATALOG)));
    when(schemas.getSchema(names.get(SCHEMA))).thenReturn(new SchemaInfo().schemaId(id(SCHEMA)));
    when(tables.getTable(names.get(TABLE))).thenReturn(new TableInfo().tableId(id(TABLE)));
    when(functions.getFunction(names.get(FUNCTION)))
        .thenReturn(new FunctionInfo().functionId(id(FUNCTION)));
    when(volumes.getVolume(names.get(VOLUME))).thenReturn(new VolumeInfo().volumeId(id(VOLUME)));
    when(models.getRegisteredModel(names.get(REGISTERED_MODEL)))
        .thenReturn(new RegisteredModelInfo().id(id(REGISTERED_MODEL)));
    when(externalLocations.getExternalLocation(names.get(EXTERNAL_LOCATION)))
        .thenReturn(new ExternalLocationInfo().id(id(EXTERNAL_LOCATION)));
    when(credentials.getCredential(names.get(CREDENTIAL)))
        .thenReturn(new CredentialInfo().id(id(CREDENTIAL)));
    when(authorizer.getHierarchyParent(resources.get(TABLE))).thenReturn(resources.get(SCHEMA));
    when(authorizer.getHierarchyParent(resources.get(SCHEMA))).thenReturn(resources.get(CATALOG));
    service = new PermissionService(authorizer, repositories, mock(ServerProperties.class));
  }

  @ParameterizedTest
  @CsvSource({
    "TABLE, SCHEMA, true",
    "TABLE, SCHEMA, false",
    "TABLE, CATALOG, true",
    "TABLE, CATALOG, false",
    "SCHEMA, CATALOG, true",
    "SCHEMA, CATALOG, false",
    "CATALOG, CATALOG, true",
    "CATALOG, CATALOG, false"
  })
  public void ownershipChecksUseAncestorTypes(
      SecurableType resourceType, SecurableType ownerType, boolean allowed) throws Exception {
    UUID resourceId = resources.get(resourceType);
    when(authorizer.authorize(principalId, ownerType, resources.get(ownerType), OWNER))
        .thenReturn(allowed);
    when(authorizer.listAuthorizations(resourceId))
        .thenReturn(Map.of(principalId, List.of(SELECT), otherPrincipalId, List.of(SELECT)));
    when(authorizer.listAuthorizations(principalId, resourceId)).thenReturn(List.of(SELECT));

    HttpResponse response =
        switch (resourceType) {
          case TABLE -> service.getTableAuthorization(names.get(TABLE), Optional.empty());
          case SCHEMA -> service.getSchemaAuthorization(names.get(SCHEMA), Optional.empty());
          case CATALOG -> service.getCatalogAuthorization(names.get(CATALOG), Optional.empty());
          default ->
              throw new IllegalArgumentException("Unexpected test resource: " + resourceType);
        };
    PermissionsList permissions =
        JsonUtils.getInstance()
            .readValue(response.aggregate().join().contentUtf8(), PermissionsList.class);

    assertThat(permissions.getPrivilegeAssignments())
        .extracting(PrivilegeAssignment::getPrincipal)
        .containsExactlyInAnyOrderElementsOf(allowed ? List.of(ALICE, BOB) : List.of(ALICE));
    verify(authorizer).authorize(principalId, ownerType, resources.get(ownerType), OWNER);
  }

  @ParameterizedTest
  @EnumSource(SecurableType.class)
  public void principalParameterFiltersEverySecurableType(SecurableType type) throws Exception {
    stubGrantsAndCallerVisibility(type, /* canReadAllGrants= */ true);

    Server server = Server.builder().http(0).annotatedService("/permissions", service).build();
    server.start().join();
    try {
      WebClient client = WebClient.of("http://127.0.0.1:" + server.activeLocalPort());
      String path = "/permissions/" + type.getValue() + "/" + names.get(type);
      assertThat(getPermissions(client, path + "?principal=" + BOB)).containsExactly(BOB_MODIFY);
      assertThat(getPermissions(client, path + "?principal=")).isEmpty();
      assertThat(getPermissions(client, path)).containsExactlyInAnyOrder(ALICE_SELECT, BOB_MODIFY);
    } finally {
      server.stop().join();
    }
  }

  @ParameterizedTest
  @CsvSource({
    "true, alice@example.com, alice@example.com",
    "true, bob@example.com, bob@example.com",
    "true, BOB@example.com,",
    "true, carol@example.com,",
    "true, '',",
    "false, alice@example.com, alice@example.com",
    "false, bob@example.com,",
    "false, carol@example.com,"
  })
  public void principalParameterOnlyNarrowsTheGrantsTheCallerCanSee(
      boolean canReadAllGrants, String principal, String expectedEmail) throws Exception {
    stubGrantsAndCallerVisibility(CATALOG, canReadAllGrants);
    UUID userWithoutEmail = UUID.randomUUID();
    when(users.getUser(userWithoutEmail.toString())).thenReturn(new User());
    when(authorizer.listAuthorizations(resources.get(CATALOG)))
        .thenReturn(
            Map.of(
                principalId,
                List.of(SELECT),
                otherPrincipalId,
                List.of(MODIFY),
                userWithoutEmail,
                List.of(SELECT)));

    HttpResponse response =
        service.getCatalogAuthorization(names.get(CATALOG), Optional.of(principal));
    PermissionsList permissions =
        JsonUtils.getInstance()
            .readValue(response.aggregate().join().contentUtf8(), PermissionsList.class);

    assertThat(permissions.getPrivilegeAssignments())
        .containsExactlyElementsOf(
            Optional.ofNullable(expectedEmail)
                .map(email -> List.of(ASSIGNMENTS_BY_EMAIL.get(email)))
                .orElse(List.of()));
  }

  private void stubGrantsAndCallerVisibility(SecurableType type, boolean canReadAllGrants) {
    UUID resourceId = resources.get(type);
    when(authorizer.authorize(principalId, METASTORE, resources.get(METASTORE), OWNER))
        .thenReturn(canReadAllGrants);
    when(authorizer.listAuthorizations(resourceId))
        .thenReturn(Map.of(principalId, List.of(SELECT), otherPrincipalId, List.of(MODIFY)));
    when(authorizer.listAuthorizations(principalId, resourceId)).thenReturn(List.of(SELECT));
  }

  private static List<PrivilegeAssignment> getPermissions(WebClient client, String path)
      throws Exception {
    AggregatedHttpResponse response = client.get(path).aggregate().join();
    assertThat(response.status()).isEqualTo(HttpStatus.OK);
    return JsonUtils.getInstance()
        .readValue(response.contentUtf8(), PermissionsList.class)
        .getPrivilegeAssignments();
  }

  private String id(SecurableType type) {
    return resources.get(type).toString();
  }
}
