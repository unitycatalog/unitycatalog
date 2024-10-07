package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.PermissionsChange;
import io.unitycatalog.server.model.PermissionsList;
import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.PrivilegeAssignment;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.UpdatePermissions;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.FunctionRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.ModelRepository;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.VolumeRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.IdentityUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.FUNCTION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.model.SecurableType.VOLUME;

@ExceptionHandler(GlobalExceptionHandler.class)
public class PermissionService {

  private final UnityCatalogAuthorizer authorizer;
  private static final MetastoreRepository METASTORE_REPOSITORY = MetastoreRepository.getInstance();
  private static final UserRepository USER_REPOSITORY = UserRepository.getInstance();
  private static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getInstance();
  private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getInstance();
  private static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();
  private static final FunctionRepository FUNCTION_REPOSITORY = FunctionRepository.getInstance();
  private static final VolumeRepository VOLUME_REPOSITORY = VolumeRepository.getInstance();
  private static final ModelRepository MODEL_REPOSITORY = ModelRepository.getInstance();

  public PermissionService(UnityCatalogAuthorizer authorizer) {
    this.authorizer = authorizer;
  }

  // TODO: Refactor these endpoints to use a common method with dynamic resource id lookup
  @Get("/metastore/{name}")
  public HttpResponse getMetastoreAuthorization(
      @Param("name") String name) {
    return getAuthorization(METASTORE, name);
  }

  @Get("/catalog/{name}")
  public HttpResponse getCatalogAuthorization(
      @Param("name") String name) {
    return getAuthorization(CATALOG, name);
  }

  @Get("/schema/{name}")
  public HttpResponse getSchemaAuthorization(
      @Param("name") String name) {
    return getAuthorization(SCHEMA, name);
  }

  @Get("/table/{name}")
  public HttpResponse getTableAuthorization(
      @Param("name") String name) {
    return getAuthorization(TABLE, name);
  }

  @Get("/function/{name}")
  public HttpResponse getFunctionAuthorization(
      @Param("name") String name) {
    return getAuthorization(FUNCTION, name);
  }

  @Get("/volume/{name}")
  public HttpResponse getVolumeAuthorization(
      @Param("name") String name) {
    return getAuthorization(VOLUME, name);
  }

  @Get("/registered_model/{name}")
  public HttpResponse getRegisteredModelAuthorization(
      @Param("name") String name) {
    return getAuthorization(REGISTERED_MODEL, name);
  }

  private HttpResponse getAuthorization(
      SecurableType securableType, String name) {

    // Only show permissions for the authenticated identity unless they are the owner
    // or if the authenticated identity is the owner of the parent resource(s)
    // of the resource or the metastore itself.

    UUID resourceId = getResourceId(securableType, name);
    UUID principalId = IdentityUtils.findPrincipalId();

    // TODO: could be more explicit about the hierarchy here.
    // For now this is sufficient in that it covers owner on resources parentage.
    UUID parentId = authorizer.getHierarchyParent(resourceId);
    UUID grandparentId = (parentId != null) ? authorizer.getHierarchyParent(parentId):null;

    boolean isOwner =
            authorizer.authorize(principalId, METASTORE_REPOSITORY.getMetastoreId(), Privileges.OWNER) ||
            authorizer.authorize(principalId, resourceId, Privileges.OWNER) ||
            (parentId != null && authorizer.authorize(principalId, parentId, Privileges.OWNER)) ||
            (grandparentId != null && authorizer.authorize(principalId, grandparentId, Privileges.OWNER));

    Map<UUID, List<Privileges>> authorizations =
            isOwner ?
                    authorizer.listAuthorizations(resourceId)
                    :
                    Map.of(principalId, authorizer.listAuthorizations(principalId, resourceId));

    List<PrivilegeAssignment> privilegeAssignments =
        authorizations.entrySet().stream()
            .map(
                entry -> {
                  List<Privilege> privileges =
                      entry.getValue().stream()
                          .map(Privileges::toPrivilege)
                          // mapping to Privilege may result in nulls since Privilege is a subset of
                          // Privileges, so filter them out.
                          .filter(Objects::nonNull)
                          .toList();
                  return new PrivilegeAssignment()
                      .principal(USER_REPOSITORY.getUser(entry.getKey().toString()).getEmail())
                      .privileges(privileges);
                })
            .filter(assignment -> !assignment.getPrivileges().isEmpty())
            .collect(Collectors.toList());

    return HttpResponse.ofJson(new PermissionsList().privilegeAssignments(privilegeAssignments));
  }

  // TODO: Refactor these endpoints to use a common method with dynamic resource id lookup
  @Patch("/metastore/{name}")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeKey(METASTORE)
  public HttpResponse updateMetastoreAuthorization(
      @Param("name") String name, UpdatePermissions request) {
    return updateAuthorization(METASTORE, name, request);
  }

  @Patch("/catalog/{name}")
  @AuthorizeExpression(
      "#authorize(#principal, #metastore, OWNER) || #authorize(#principal, #catalog, OWNER)")
  @AuthorizeKey(METASTORE)
  public HttpResponse updateCatalogAuthorization(
      @Param("name") @AuthorizeKey(CATALOG) String name, UpdatePermissions request) {
    return updateAuthorization(CATALOG, name, request);
  }

  @Patch("/schema/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateSchemaAuthorization(
      @Param("name") @AuthorizeKey(SCHEMA) String name, UpdatePermissions request) {
    return updateAuthorization(SCHEMA, name, request);
  }

  @Patch("/table/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #table, OWNER))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateTableAuthorization(
      @Param("name") @AuthorizeKey(TABLE) String name, UpdatePermissions request) {
    return updateAuthorization(TABLE, name, request);
  }

  @Patch("/function/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #function, OWNER))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateFunctionAuthorization(
      @Param("name") @AuthorizeKey(FUNCTION) String name, UpdatePermissions request) {
    return updateAuthorization(FUNCTION, name, request);
  }

  @Patch("/volume/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #volume, OWNER))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateVolumeAuthorization(
      @Param("name") @AuthorizeKey(VOLUME) String name, UpdatePermissions request) {
    return updateAuthorization(VOLUME, name, request);
  }

  @Patch("/registered_model/{name}")
  @AuthorizeExpression(
      "#authorize(#principal, #metastore, OWNER) || #authorize(#principal, #registered_model, OWNER)")
  @AuthorizeKey(METASTORE)
  public HttpResponse updateRegisteredModelAuthorization(
      @Param("name") @AuthorizeKey(REGISTERED_MODEL) String name, UpdatePermissions request) {
    return updateAuthorization(REGISTERED_MODEL, name, request);
  }

  private HttpResponse updateAuthorization(
      SecurableType securableType, String name, UpdatePermissions request) {
    UUID resourceId = getResourceId(securableType, name);
    List<PermissionsChange> changes = request.getChanges();
    Set<UUID> principalIds = new HashSet<>();
    changes.forEach(
        change -> {
          String principal = change.getPrincipal();
          User user = USER_REPOSITORY.getUserByEmail(principal);
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
                          .map(Privileges::toPrivilege)
                          // mapping to Privilege may result in nulls since Privilege is a subset of
                          // Privileges, so filter them out.
                          .filter(Objects::nonNull)
                          .toList();
                  return new PrivilegeAssignment()
                      .principal(USER_REPOSITORY.getUser(entry.getKey().toString()).getEmail())
                      .privileges(privileges);
                })
            .filter(assignment -> !assignment.getPrivileges().isEmpty())
            .collect(Collectors.toList());

    return HttpResponse.ofJson(new PermissionsList().privilegeAssignments(privilegeAssignments));
  }

  private UUID getResourceId(SecurableType securableType, String name) {

    String resourceId = switch (securableType) {
      case METASTORE -> METASTORE_REPOSITORY.getMetastoreId().toString();
      case CATALOG -> CATALOG_REPOSITORY.getCatalog(name).getId();
      case SCHEMA -> SCHEMA_REPOSITORY.getSchema(name).getSchemaId();
      case TABLE -> TABLE_REPOSITORY.getTable(name).getTableId();
      case FUNCTION -> FUNCTION_REPOSITORY.getFunction(name).getFunctionId();
      case VOLUME -> VOLUME_REPOSITORY.getVolume(name).getVolumeId();
      case REGISTERED_MODEL -> MODEL_REPOSITORY.getRegisteredModel(name).getId();
      default -> throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Unknown resource type");
    };

    return UUID.fromString(Objects.requireNonNull(resourceId));
  }
}
