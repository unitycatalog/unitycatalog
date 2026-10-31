package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.persist.model.Privileges.OWNER;
import static io.unitycatalog.server.persist.model.Privileges.SELECT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linecorp.armeria.common.HttpResponse;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.PermissionsList;
import io.unitycatalog.server.model.PrivilegeAssignment;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.utils.JsonUtils;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class PermissionServiceHierarchyTest {
  private final UnityCatalogAuthorizer authorizer = mock();
  private final UUID principalId = UUID.randomUUID();
  private final UUID otherPrincipalId = UUID.randomUUID();
  private final Map<SecurableType, UUID> resources =
      Map.of(
          METASTORE, UUID.randomUUID(),
          CATALOG, UUID.randomUUID(),
          SCHEMA, UUID.randomUUID(),
          TABLE, UUID.randomUUID());
  private PermissionService service;

  @BeforeEach
  public void setUp() {
    Repositories repositories = mock();
    UserRepository users = mock();
    MetastoreRepository metastores = mock();
    CatalogRepository catalogs = mock();
    SchemaRepository schemas = mock();
    TableRepository tables = mock();
    when(repositories.getUserRepository()).thenReturn(users);
    when(repositories.getMetastoreRepository()).thenReturn(metastores);
    when(repositories.getCatalogRepository()).thenReturn(catalogs);
    when(repositories.getSchemaRepository()).thenReturn(schemas);
    when(repositories.getTableRepository()).thenReturn(tables);
    when(users.findPrincipalId()).thenReturn(principalId);
    when(users.getUser(principalId.toString())).thenReturn(new User().email("alice@example.com"));
    when(users.getUser(otherPrincipalId.toString()))
        .thenReturn(new User().email("bob@example.com"));
    when(metastores.getMetastoreId()).thenReturn(resources.get(METASTORE));
    when(catalogs.getCatalog("analytics"))
        .thenReturn(new CatalogInfo().id(resources.get(CATALOG).toString()));
    when(schemas.getSchema("analytics.reports"))
        .thenReturn(new SchemaInfo().schemaId(resources.get(SCHEMA).toString()));
    when(tables.getTable("analytics.reports.orders"))
        .thenReturn(new TableInfo().tableId(resources.get(TABLE).toString()));
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
          case TABLE -> service.getTableAuthorization("analytics.reports.orders");
          case SCHEMA -> service.getSchemaAuthorization("analytics.reports");
          case CATALOG -> service.getCatalogAuthorization("analytics");
          default ->
              throw new IllegalArgumentException("Unexpected test resource: " + resourceType);
        };
    PermissionsList permissions =
        JsonUtils.getInstance()
            .readValue(response.aggregate().join().contentUtf8(), PermissionsList.class);

    assertThat(permissions.getPrivilegeAssignments())
        .extracting(PrivilegeAssignment::getPrincipal)
        .containsExactlyInAnyOrderElementsOf(
            allowed
                ? List.of("alice@example.com", "bob@example.com")
                : List.of("alice@example.com"));
    verify(authorizer).authorize(principalId, ownerType, resources.get(ownerType), OWNER);
  }
}
