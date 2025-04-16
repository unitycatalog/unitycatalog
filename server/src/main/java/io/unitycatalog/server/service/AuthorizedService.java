package io.unitycatalog.server.service;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.UUID;
import lombok.SneakyThrows;

/**
 * Abstract service class that provides common authorization functionality for all Unity Catalog
 * services.
 */
public abstract class AuthorizedService {
  protected final UnityCatalogAuthorizer authorizer;
  protected final UnityAccessEvaluator evaluator;
  protected final UserRepository userRepository;

  @SneakyThrows
  protected AuthorizedService(UnityCatalogAuthorizer authorizer, UserRepository userRepository) {
    this.authorizer = authorizer;
    this.evaluator = new UnityAccessEvaluator(authorizer);
    this.userRepository = userRepository;
  }

  /**
   * Initializes basic authorization for a resource by granting owner privileges to the current
   * principal.
   *
   * @param resourceId String ID of the resource to grant permission for
   */
  protected void initializeBasicAuthorization(String resourceId) {
    UUID principalId = userRepository.findPrincipalId();
    authorizer.grantAuthorization(principalId, UUID.fromString(resourceId), Privileges.OWNER);
  }

  /**
   * Initializes hierarchical authorization for a resource by granting owner privileges to the
   * current principal and establishing a parent-child relationship.
   *
   * @param resourceId String ID of the resource to grant permission for
   * @param parentId String ID of the parent resource
   */
  protected void initializeHierarchicalAuthorization(String resourceId, String parentId) {
    initializeBasicAuthorization(resourceId);
    authorizer.addHierarchyChild(UUID.fromString(parentId), UUID.fromString(resourceId));
  }

  /**
   * Removes all authorizations for a resource.
   *
   * @param resourceId String ID of the resource to remove authorizations for
   */
  protected void removeAuthorizations(String resourceId) {
    authorizer.clearAuthorizationsForResource(UUID.fromString(resourceId));
  }

  /**
   * Removes all authorizations for a resource and removes the parent-child relationship.
   *
   * @param resourceId String ID of the resource to remove authorizations for
   * @param parentId String ID of the parent resource
   */
  protected void removeHierarchicalAuthorizations(String resourceId, String parentId) {
    removeAuthorizations(resourceId);
    authorizer.removeHierarchyChild(UUID.fromString(parentId), UUID.fromString(resourceId));
  }
}
