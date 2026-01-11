package io.unitycatalog.server.service;

import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.auth.decorator.ResultFilter;
import io.unitycatalog.server.auth.decorator.UnityAccessDecorator;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;

/**
 * Abstract service class that provides common authorization functionality for all Unity Catalog
 * services.
 */
public abstract class AuthorizedService {
  protected final UnityCatalogAuthorizer authorizer;
  protected final UserRepository userRepository;
  protected final KeyMapper keyMapper;
  protected final ServerProperties serverProperties;

  @SneakyThrows
  protected AuthorizedService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    this.authorizer = authorizer;
    this.userRepository = repositories.getUserRepository();
    this.keyMapper = repositories.getKeyMapper();
    this.serverProperties = serverProperties;
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

  /**
   * Applies authorization filtering to a list of resources.
   *
   * <p>This method should be called by service methods annotated with
   * {@code @ResponseAuthorizeFilter} to filter the response list based on the user's permissions.
   * When authorization is enabled, it retrieves the {@link ResultFilter} from the request context
   * and applies it to the provided list, removing items that the user is not authorized to access.
   *
   * <p><b>IMPORTANT:</b> Service methods annotated with {@code @ResponseAuthorizeFilter} MUST call
   * this method before returning a successful response. Failure to do so will result in a security
   * exception being thrown by {@link UnityAccessDecorator}.
   *
   * @param securableType The type of resources being filtered (TABLE, VOLUME, FUNCTION, etc.)
   * @param items The list of items to filter. The list is modified in-place by removing
   *     unauthorized items.
   * @param <T> The type of items in the list
   */
  protected <T> void applyResponseFilter(SecurableType securableType, List<T> items) {
    if (serverProperties.isAuthorizationEnabled()) {
      ResultFilter resultFilter =
          ServiceRequestContext.current().attr(UnityAccessDecorator.RESULT_FILTER_ATTR);
      resultFilter.filter(securableType, items);
    }
  }
}
