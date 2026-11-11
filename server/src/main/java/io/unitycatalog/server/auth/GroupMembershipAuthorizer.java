package io.unitycatalog.server.auth;

import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.GroupRepository;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Lets a user act with the grants of the groups they are a member of, OWNER included. A check the
 * user's own grants deny is repeated for each of their groups; membership is read from the database
 * at that point, so a membership change applies from the next check on every server instance.
 *
 * <p>Only decisions are widened. Grants, hierarchy and listings stay per principal.
 */
public final class GroupMembershipAuthorizer implements UnityCatalogAuthorizer, AutoCloseable {
  private final UnityCatalogAuthorizer delegate;
  private final GroupRepository groupRepository;

  public GroupMembershipAuthorizer(
      UnityCatalogAuthorizer delegate, GroupRepository groupRepository) {
    this.delegate = delegate;
    this.groupRepository = groupRepository;
  }

  @Override
  public boolean authorize(
      UUID principal, SecurableType resourceType, UUID resource, Privileges action) {
    return delegate.authorize(principal, resourceType, resource, action)
        || groupRepository.listGroupIdsOfUser(principal).stream()
            .anyMatch(group -> delegate.authorize(group, resourceType, resource, action));
  }

  @Override
  public boolean authorizeAny(
      UUID principal, SecurableType resourceType, UUID resource, Privileges... actions) {
    return delegate.authorizeAny(principal, resourceType, resource, actions)
        || groupRepository.listGroupIdsOfUser(principal).stream()
            .anyMatch(group -> delegate.authorizeAny(group, resourceType, resource, actions));
  }

  /** Each action may come from the user or from any one of their groups. */
  @Override
  public boolean authorizeAll(
      UUID principal, SecurableType resourceType, UUID resource, Privileges... actions) {
    if (delegate.authorizeAll(principal, resourceType, resource, actions)) {
      return true;
    }
    List<UUID> groups = groupRepository.listGroupIdsOfUser(principal);
    return Arrays.stream(actions)
        .allMatch(
            action ->
                delegate.authorize(principal, resourceType, resource, action)
                    || groups.stream()
                        .anyMatch(
                            group -> delegate.authorize(group, resourceType, resource, action)));
  }

  @Override
  public boolean grantAuthorization(UUID principal, UUID resource, Privileges action) {
    return delegate.grantAuthorization(principal, resource, action);
  }

  @Override
  public boolean revokeAuthorization(UUID principal, UUID resource, Privileges action) {
    return delegate.revokeAuthorization(principal, resource, action);
  }

  @Override
  public boolean clearAuthorizationsForPrincipal(UUID principal) {
    return delegate.clearAuthorizationsForPrincipal(principal);
  }

  @Override
  public boolean clearAuthorizationsForResource(UUID resource) {
    return delegate.clearAuthorizationsForResource(resource);
  }

  @Override
  public boolean addHierarchyChild(UUID parent, UUID child) {
    return delegate.addHierarchyChild(parent, child);
  }

  @Override
  public boolean removeHierarchyChild(UUID parent, UUID child) {
    return delegate.removeHierarchyChild(parent, child);
  }

  @Override
  public boolean removeHierarchyChildren(UUID resource) {
    return delegate.removeHierarchyChildren(resource);
  }

  @Override
  public UUID getHierarchyParent(UUID resource) {
    return delegate.getHierarchyParent(resource);
  }

  @Override
  public List<Privileges> listAuthorizations(UUID principal, UUID resource) {
    return delegate.listAuthorizations(principal, resource);
  }

  @Override
  public Map<UUID, List<Privileges>> listAuthorizations(UUID resource) {
    return delegate.listAuthorizations(resource);
  }

  // TODO(#2149): forward the new reloadAuthorizations() to the delegate once #2149 adds it.
  @Override
  public boolean refreshAuthorizations(long operationStartNanos) {
    return delegate.refreshAuthorizations(operationStartNanos);
  }

  @Override
  public void close() throws Exception {
    if (delegate instanceof AutoCloseable closeable) {
      closeable.close();
    }
  }
}
