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
  public boolean grantAuthorization(UUID principal, UUID resource, Privileges action);

  public boolean revokeAuthorization(UUID principal, UUID resource, Privileges action);

  public boolean clearAuthorizationsForPrincipal(UUID principal);

  public boolean clearAuthorizationsForResource(UUID resource);

  public boolean addHierarchyChild(UUID parent, UUID child);

  public boolean removeHierarchyChild(UUID parent, UUID child);

  public boolean removeHierarchyChildren(UUID resource);

  public UUID getHierarchyParent(UUID resource);

  public boolean authorize(UUID principal, UUID resource, Privileges action);

  public boolean authorizeAny(UUID principal, UUID resource, Privileges... actions);

  public boolean authorizeAll(UUID principal, UUID resource, Privileges... actions);

  public List<Privileges> listAuthorizations(UUID principal, UUID resource);

  public Map<UUID, List<Privileges>> listAuthorizations(UUID resource);
}
