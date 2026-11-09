package io.unitycatalog.server.service;

import static io.unitycatalog.server.auth.decorator.UnityAccessUtil.getHierarchyParentType;
import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.CREDENTIAL;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.FUNCTION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.model.SecurableType.VOLUME;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.PermissionsChange;
import io.unitycatalog.server.model.PermissionsList;
import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.PrivilegeAssignment;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.UpdatePermissions;
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
import io.unitycatalog.server.utils.ServerProperties;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class PermissionService implements UnityCatalogRestService {

  private final UnityCatalogAuthorizer authorizer;
  private final boolean strictPrivilegeChecks;
  private final MetastoreRepository metastoreRepository;
  private final UserRepository userRepository;
  private final CatalogRepository catalogRepository;
  private final SchemaRepository schemaRepository;
  private final TableRepository tableRepository;
  private final FunctionRepository functionRepository;
  private final VolumeRepository volumeRepository;
  private final ModelRepository modelRepository;
  private final ExternalLocationRepository externalLocationRepository;
  private final CredentialRepository credentialRepository;

  public PermissionService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    this.authorizer = authorizer;
    this.strictPrivilegeChecks = serverProperties.isStrictPrivilegeChecksEnabled();
    this.metastoreRepository = repositories.getMetastoreRepository();
    this.userRepository = repositories.getUserRepository();
    this.catalogRepository = repositories.getCatalogRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.tableRepository = repositories.getTableRepository();
    this.functionRepository = repositories.getFunctionRepository();
    this.volumeRepository = repositories.getVolumeRepository();
    this.modelRepository = repositories.getModelRepository();
    this.externalLocationRepository = repositories.getExternalLocationRepository();
    this.credentialRepository = repositories.getCredentialRepository();
  }

  // TODO: Refactor these endpoints to use a common method with dynamic resource id lookup
  @Get("/metastore/{name}")
  @AuthorizeExpression(AuthorizeExpressions.GET_RESOURCE_AUTHORIZATION)
  public HttpResponse getMetastoreAuthorization(@Param("name") String name) {
    return getAuthorization(METASTORE, name);
  }

  @Get("/catalog/{name}")
  @AuthorizeExpression(AuthorizeExpressions.GET_RESOURCE_AUTHORIZATION)
  public HttpResponse getCatalogAuthorization(@Param("name") String name) {
    return getAuthorization(CATALOG, name);
  }

  @Get("/schema/{name}")
  @AuthorizeExpression(AuthorizeExpressions.GET_RESOURCE_AUTHORIZATION)
  public HttpResponse getSchemaAuthorization(@Param("name") String name) {
    return getAuthorization(SCHEMA, name);
  }

  @Get("/table/{name}")
  @AuthorizeExpression(AuthorizeExpressions.GET_RESOURCE_AUTHORIZATION)
  public HttpResponse getTableAuthorization(@Param("name") String name) {
    return getAuthorization(TABLE, name);
  }

  @Get("/function/{name}")
  @AuthorizeExpression(AuthorizeExpressions.GET_RESOURCE_AUTHORIZATION)
  public HttpResponse getFunctionAuthorization(@Param("name") String name) {
    return getAuthorization(FUNCTION, name);
  }

  @Get("/volume/{name}")
  @AuthorizeExpression(AuthorizeExpressions.GET_RESOURCE_AUTHORIZATION)
  public HttpResponse getVolumeAuthorization(@Param("name") String name) {
    return getAuthorization(VOLUME, name);
  }

  @Get("/registered_model/{name}")
  @AuthorizeExpression(AuthorizeExpressions.GET_RESOURCE_AUTHORIZATION)
  public HttpResponse getRegisteredModelAuthorization(@Param("name") String name) {
    return getAuthorization(REGISTERED_MODEL, name);
  }

  @Get("/external_location/{name}")
  @AuthorizeExpression(AuthorizeExpressions.GET_RESOURCE_AUTHORIZATION)
  public HttpResponse getExternalLocationAuthorization(@Param("name") String name) {
    return getAuthorization(EXTERNAL_LOCATION, name);
  }

  @Get("/credential/{name}")
  @AuthorizeExpression(AuthorizeExpressions.GET_RESOURCE_AUTHORIZATION)
  public HttpResponse getCredentialAuthorization(@Param("name") String name) {
    return getAuthorization(CREDENTIAL, name);
  }

  private HttpResponse getAuthorization(SecurableType securableType, String name) {

    // Owners and metadata readers can view all direct grants. Other users see their own grants.

    UUID resourceId = getResourceId(securableType, name);
    UUID principalId = userRepository.findPrincipalId();

    // TODO: could be more explicit about the hierarchy here.
    // For now this is sufficient in that it covers owner on resources parentage.
    UUID parentId = authorizer.getHierarchyParent(resourceId);
    UUID grandparentId = (parentId != null) ? authorizer.getHierarchyParent(parentId) : null;
    SecurableType parentType = getHierarchyParentType(securableType);
    SecurableType grandparentType = parentType != null ? getHierarchyParentType(parentType) : null;

    boolean isOwner =
        authorizer.authorize(
                principalId, METASTORE, metastoreRepository.getMetastoreId(), Privileges.OWNER)
            || authorizer.authorize(principalId, securableType, resourceId, Privileges.OWNER)
            || (parentType != null
                && parentId != null
                && authorizer.authorize(principalId, parentType, parentId, Privileges.OWNER))
            || (grandparentType != null
                && grandparentId != null
                && authorizer.authorize(
                    principalId, grandparentType, grandparentId, Privileges.OWNER));

    boolean canReadAllGrants =
        isOwner
            || authorizer.authorize(
                principalId,
                METASTORE,
                metastoreRepository.getMetastoreId(),
                Privileges.READ_METADATA)
            || switch (securableType) {
              case METASTORE, CATALOG, CREDENTIAL, EXTERNAL_LOCATION ->
                  authorizer.authorize(
                      principalId, securableType, resourceId, Privileges.READ_METADATA);
              case SCHEMA ->
                  parentId != null
                      && (authorizer.authorize(
                              principalId, CATALOG, parentId, Privileges.READ_METADATA)
                          || (authorizer.authorize(
                                  principalId, CATALOG, parentId, Privileges.USE_CATALOG)
                              && authorizer.authorize(
                                  principalId, SCHEMA, resourceId, Privileges.READ_METADATA)));
              case TABLE, FUNCTION, VOLUME, REGISTERED_MODEL ->
                  parentId != null
                      && grandparentId != null
                      && (authorizer.authorize(
                              principalId, CATALOG, grandparentId, Privileges.READ_METADATA)
                          || (authorizer.authorize(
                                  principalId, CATALOG, grandparentId, Privileges.USE_CATALOG)
                              && (authorizer.authorize(
                                      principalId, SCHEMA, parentId, Privileges.READ_METADATA)
                                  || (authorizer.authorize(
                                          principalId, SCHEMA, parentId, Privileges.USE_SCHEMA)
                                      && authorizer.authorize(
                                          principalId,
                                          securableType,
                                          resourceId,
                                          Privileges.READ_METADATA)))));
            };

    Map<UUID, List<Privileges>> authorizations =
        canReadAllGrants
            ? authorizer.listAuthorizations(resourceId)
            : Map.of(principalId, authorizer.listAuthorizations(principalId, resourceId));

    List<PrivilegeAssignment> privilegeAssignments =
        authorizations.entrySet().stream()
            .map(
                entry -> {
                  List<Privilege> privileges =
                      entry.getValue().stream()
                          .<Privilege>map(Privileges::toPrivilege)
                          // mapping to Privilege may result in nulls since Privilege is a subset of
                          // Privileges, so filter them out.
                          .filter(Objects::nonNull)
                          .toList();
                  return new PrivilegeAssignment()
                      .principal(userRepository.getUser(entry.getKey().toString()).getEmail())
                      .privileges(privileges);
                })
            .filter(assignment -> !assignment.getPrivileges().isEmpty())
            .collect(Collectors.toList());

    return HttpResponse.ofJson(new PermissionsList().privilegeAssignments(privilegeAssignments));
  }

  // TODO: Refactor these endpoints to use a common method with dynamic resource id lookup
  @Patch("/metastore/{name}")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateMetastoreAuthorization(
      @Param("name") String name, UpdatePermissions request) {
    return updateAuthorization(METASTORE, name, request);
  }

  @Patch("/catalog/{name}")
  @AuthorizeExpression(
      "#authorize(#principal, #metastore, OWNER) || #authorize(#principal, #catalog, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateCatalogAuthorization(
      @Param("name") @AuthorizeResourceKey(CATALOG) String name, UpdatePermissions request) {
    return updateAuthorization(CATALOG, name, request);
  }

  @Patch("/schema/{name}")
  @AuthorizeExpression(
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateSchemaAuthorization(
      @Param("name") @AuthorizeResourceKey(SCHEMA) String name, UpdatePermissions request) {
    return updateAuthorization(SCHEMA, name, request);
  }

  @Patch("/table/{name}")
  @AuthorizeExpression(
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
      (#authorize(#principal, #catalog, USE_CATALOG) &&
          #authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #table, OWNER))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateTableAuthorization(
      @Param("name") @AuthorizeResourceKey(TABLE) String name, UpdatePermissions request) {
    return updateAuthorization(TABLE, name, request);
  }

  @Patch("/function/{name}")
  @AuthorizeExpression(
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
      (#authorize(#principal, #catalog, USE_CATALOG) &&
          #authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #function, OWNER))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateFunctionAuthorization(
      @Param("name") @AuthorizeResourceKey(FUNCTION) String name, UpdatePermissions request) {
    return updateAuthorization(FUNCTION, name, request);
  }

  @Patch("/volume/{name}")
  @AuthorizeExpression(
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
      (#authorize(#principal, #catalog, USE_CATALOG) &&
          #authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #volume, OWNER))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateVolumeAuthorization(
      @Param("name") @AuthorizeResourceKey(VOLUME) String name, UpdatePermissions request) {
    return updateAuthorization(VOLUME, name, request);
  }

  @Patch("/registered_model/{name}")
  @AuthorizeExpression(
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
      (#authorize(#principal, #catalog, USE_CATALOG) &&
          #authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #registered_model, OWNER))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateRegisteredModelAuthorization(
      @Param("name") @AuthorizeResourceKey(REGISTERED_MODEL) String name,
      UpdatePermissions request) {
    return updateAuthorization(REGISTERED_MODEL, name, request);
  }

  @Patch("/external_location/{name}")
  @AuthorizeExpression(
      "#authorize(#principal, #metastore, OWNER) || #authorize(#principal, #external_location,"
          + " OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateExternalLocationAuthorization(
      @Param("name") @AuthorizeResourceKey(EXTERNAL_LOCATION) String name,
      UpdatePermissions request) {
    return updateAuthorization(EXTERNAL_LOCATION, name, request);
  }

  @Patch("/credential/{name}")
  @AuthorizeExpression(
      "#authorize(#principal, #metastore, OWNER) || #authorize(#principal, #credential, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateCredentialAuthorization(
      @Param("name") @AuthorizeResourceKey(CREDENTIAL) String name, UpdatePermissions request) {
    return updateAuthorization(CREDENTIAL, name, request);
  }

  private HttpResponse updateAuthorization(
      SecurableType securableType, String name, UpdatePermissions request) {
    UUID resourceId = getResourceId(securableType, name);
    List<PermissionsChange> changes = request.getChanges();
    if (strictPrivilegeChecks) {
      validateAddedPrivileges(securableType, changes);
    }
    Set<UUID> principalIds = new HashSet<>();
    changes.forEach(
        change -> {
          String principal = change.getPrincipal();
          User user = userRepository.getUserByEmail(principal);
          UUID principalId = UUID.fromString(Objects.requireNonNull(user.getId()));
          principalIds.add(principalId);
          change
              .getAdd()
              .forEach(
                  privilege ->
                      //  Privileges should always be a superset of Privilege so this _should_
                      // always be non-null but let's be safe anyway.
                      Optional.ofNullable(Privileges.fromPrivilege(privilege))
                          .map(p -> authorizer.grantAuthorization(principalId, resourceId, p)));
          change
              .getRemove()
              .forEach(
                  privilege ->
                      //  Privileges should always be a superset of Privilege so this _should_
                      // always be non-null but let's be safe anyway.
                      Optional.ofNullable(Privileges.fromPrivilege(privilege))
                          .map(p -> authorizer.revokeAuthorization(principalId, resourceId, p)));
        });

    Map<UUID, List<Privileges>> authorizations = authorizer.listAuthorizations(resourceId);
    List<PrivilegeAssignment> privilegeAssignments =
        authorizations.entrySet().stream()
            .filter(entry -> principalIds.contains(entry.getKey()))
            .map(
                entry -> {
                  List<Privilege> privileges =
                      entry.getValue().stream()
                          .<Privilege>map(Privileges::toPrivilege)
                          // mapping to Privilege may result in nulls since Privilege is a subset of
                          // Privileges, so filter them out.
                          .filter(Objects::nonNull)
                          .toList();
                  return new PrivilegeAssignment()
                      .principal(userRepository.getUser(entry.getKey().toString()).getEmail())
                      .privileges(privileges);
                })
            .filter(assignment -> !assignment.getPrivileges().isEmpty())
            .collect(Collectors.toList());

    return HttpResponse.ofJson(new PermissionsList().privilegeAssignments(privilegeAssignments));
  }

  /** Validates additions only, so grants saved on invalid object types can still be removed. */
  static void validateAddedPrivileges(
      SecurableType securableType, List<PermissionsChange> changes) {
    for (PermissionsChange change : changes) {
      for (Privilege privilege : change.getAdd()) {
        Set<SecurableType> validTypes =
            switch (privilege) {
              case BROWSE -> Set.of(CATALOG, EXTERNAL_LOCATION);
              case MANAGE ->
                  Set.of(
                      CATALOG,
                      SCHEMA,
                      TABLE,
                      VOLUME,
                      FUNCTION,
                      REGISTERED_MODEL,
                      EXTERNAL_LOCATION,
                      CREDENTIAL);
              case CREATE_CATALOG, CREATE_STORAGE_CREDENTIAL -> Set.of(METASTORE);
              case USE_CATALOG, CREATE_SCHEMA -> Set.of(CATALOG);
              case USE_SCHEMA, CREATE_TABLE, CREATE_FUNCTION, CREATE_VOLUME, CREATE_MODEL ->
                  Set.of(CATALOG, SCHEMA);
              case SELECT, MODIFY -> Set.of(CATALOG, SCHEMA, TABLE);
              case EXECUTE -> Set.of(CATALOG, SCHEMA, FUNCTION, REGISTERED_MODEL);
              case CREATE_MODEL_VERSION -> Set.of(CATALOG, SCHEMA, REGISTERED_MODEL);
              case READ_VOLUME, WRITE_VOLUME -> Set.of(CATALOG, SCHEMA, VOLUME);
              case CREATE_EXTERNAL_LOCATION -> Set.of(METASTORE, CREDENTIAL);
              case READ_FILES, WRITE_FILES, CREATE_EXTERNAL_TABLE ->
                  Set.of(EXTERNAL_LOCATION, CREDENTIAL);
              case CREATE_EXTERNAL_VOLUME, CREATE_MANAGED_STORAGE -> Set.of(EXTERNAL_LOCATION);
              case READ_METADATA ->
                  Set.of(
                      METASTORE,
                      CATALOG,
                      SCHEMA,
                      TABLE,
                      FUNCTION,
                      VOLUME,
                      REGISTERED_MODEL,
                      EXTERNAL_LOCATION,
                      CREDENTIAL);
            };
        if (!validTypes.contains(securableType)) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              "Privilege "
                  + privilege.getValue()
                  + " cannot be granted on "
                  + securableType.getValue());
        }
      }
    }
  }

  private UUID getResourceId(SecurableType securableType, String name) {

    String resourceId =
        switch (securableType) {
          case METASTORE -> metastoreRepository.getMetastoreId().toString();
          case CATALOG -> catalogRepository.getCatalog(name).getId();
          case SCHEMA -> schemaRepository.getSchema(name).getSchemaId();
          case TABLE -> tableRepository.getTable(name).getTableId();
          case FUNCTION -> functionRepository.getFunction(name).getFunctionId();
          case VOLUME -> volumeRepository.getVolume(name).getVolumeId();
          case REGISTERED_MODEL -> modelRepository.getRegisteredModel(name).getId();
          case EXTERNAL_LOCATION -> externalLocationRepository.getExternalLocation(name).getId();
          case CREDENTIAL -> credentialRepository.getCredential(name).getId();
          default ->
              throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Unknown resource type");
        };

    return UUID.fromString(Objects.requireNonNull(resourceId));
  }
}
