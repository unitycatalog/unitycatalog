package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.auth.JCasbinAuthenticator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.PrivilegeAssignment;
import io.unitycatalog.server.model.ResourceType;
import io.unitycatalog.server.model.UpdateAuthorizationChange;
import io.unitycatalog.server.model.UpdateAuthorizationRequest;
import io.unitycatalog.server.model.UpdateAuthorizationResponse;
import io.unitycatalog.server.model.User;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.UserRepository;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@ExceptionHandler(GlobalExceptionHandler.class)
public class AuthService {

  private static final JCasbinAuthenticator jCasbinAuthenticator =
      JCasbinAuthenticator.getInstance();
  private static final UserRepository USER_REPOSITORY = UserRepository.getInstance();
  private static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getInstance();

  public AuthService() {}

  @Get("/{resource_type}/{name}")
  public HttpResponse getAuthorization(
      @Param("resource_type") ResourceType resourceType,
      @Param("name") String name,
      @Param("principle") Optional<String> principle) {
    UUID resourceId = getResourceId(resourceType, name);
    Map<UUID, List<Privilege>> authorizations;
    if (principle.isPresent()) {
      User user = USER_REPOSITORY.getUserByEmail(principle.get());
      UUID principleId = UUID.fromString(Objects.requireNonNull(user.getId()));
      authorizations =
          Map.of(principleId, jCasbinAuthenticator.listAuthorizations(principleId, resourceId));
    } else {
      authorizations = jCasbinAuthenticator.listAuthorizations(resourceId);
    }

    List<PrivilegeAssignment> privilegeAssignments =
        authorizations.entrySet().stream()
            .map(
                entry ->
                    new PrivilegeAssignment()
                        .principal(entry.getKey().toString())
                        .privileges(entry.getValue()))
            .collect(Collectors.toList());

    return HttpResponse.ofJson(
        new UpdateAuthorizationResponse().privilegeAssignments(privilegeAssignments));
  }

  @Patch("/{resource_type}/{name}")
  public HttpResponse updateAuthorization(
      @Param("resource_type") ResourceType resourceType,
      @Param("name") String name,
      UpdateAuthorizationRequest request) {
    UUID resourceId = getResourceId(resourceType, name);
    List<UpdateAuthorizationChange> changes = request.getChanges();
    Set<UUID> principleIds = new HashSet<>();
    changes.forEach(
        change -> {
          String principle = change.getPrincipal();
          User user = USER_REPOSITORY.getUserByEmail(principle);
          UUID principleId = UUID.fromString(Objects.requireNonNull(user.getId()));
          principleIds.add(principleId);
          change
              .getAdd()
              .forEach(
                  privilege ->
                      jCasbinAuthenticator.grantAuthorization(principleId, resourceId, privilege));
          change
              .getRemove()
              .forEach(
                  privilege ->
                      jCasbinAuthenticator.revokeAuthorization(principleId, resourceId, privilege));
        });

    Map<UUID, List<Privilege>> authorizations = jCasbinAuthenticator.listAuthorizations(resourceId);
    List<PrivilegeAssignment> privilegeAssignments =
        authorizations.entrySet().stream()
            .filter(entry -> principleIds.contains(entry.getKey()))
            .map(
                entry ->
                    new PrivilegeAssignment()
                        .principal(entry.getKey().toString())
                        .privileges(entry.getValue()))
            .collect(Collectors.toList());

    return HttpResponse.ofJson(
        new UpdateAuthorizationResponse().privilegeAssignments(privilegeAssignments));
  }

  private UUID getResourceId(ResourceType resourceType, String name) {
    UUID resourceId;
    if (resourceType == ResourceType.CATALOG) {
      CatalogInfo catalogInfo = CATALOG_REPOSITORY.getCatalog(name);
      resourceId = UUID.fromString(Objects.requireNonNull(catalogInfo.getId()));
    } else {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Unknown resource type");
    }
    return resourceId;
  }
}
