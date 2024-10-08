package io.unitycatalog.server.auth;

import io.unitycatalog.server.persist.model.Privileges;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The definition of an authorizer for Unity Catalog.
 *
 * <p>This interface defines the methods that an authorizer for Unity Catalog must implement. An
 * authorizer is responsible for enforcing access control policies for the Unity Catalog API.
 *
 * <p>This definition provides the ability to manage parent-child relationships between resources
 * and with the appropriate implementation can enforce access control policies based on these
 * relationships.
 */
public interface UnityCatalogAuthorizer {
  boolean grantAuthorization(UUID principal, UUID resource, Privileges action);

  boolean revokeAuthorization(UUID principal, UUID resource, Privileges action);

  boolean clearAuthorizationsForPrincipal(UUID principal);

  boolean clearAuthorizationsForResource(UUID resource);

  boolean addHierarchyChild(UUID parent, UUID child);

  boolean removeHierarchyChild(UUID parent, UUID child);

  boolean removeHierarchyChildren(UUID resource);

  UUID getHierarchyParent(UUID resource);

  boolean authorize(UUID principal, UUID resource, Privileges action);

  boolean authorizeAny(UUID principal, UUID resource, Privileges... actions);

  boolean authorizeAll(UUID principal, UUID resource, Privileges... actions);

  List<Privileges> listAuthorizations(UUID principal, UUID resource);

  Map<UUID, List<Privileges>> listAuthorizations(UUID resource);
}
