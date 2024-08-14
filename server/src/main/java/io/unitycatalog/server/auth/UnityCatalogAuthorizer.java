package io.unitycatalog.server.auth;

import io.unitycatalog.server.model.Privilege;
import java.util.List;
import java.util.Map;
import java.util.UUID;

// TODO: This should be call UnityCatalogAuthorizer
public interface UnityCatalogAuthorizer {
  public boolean grantAuthorization(UUID principal, UUID resource, Privilege action);

  public boolean revokeAuthorization(UUID principal, UUID resource, Privilege action);

  public boolean clearAuthorizationsForPrincipal(UUID principal);

  public boolean clearAuthorizationsForResource(UUID resource);

  public boolean addHierarchyChild(UUID parent, UUID child);

  public boolean removeHierarchyChild(UUID parent, UUID child);

  public boolean removeHierarchyChildren(UUID resource);

  public boolean authorize(UUID principal, UUID resource, Privilege action);

  public boolean authorizeAny(UUID principal, UUID resource, Privilege... actions);

  public boolean authorizeAll(UUID principal, UUID resource, Privilege... actions);

  public List<Privilege> listAuthorizations(UUID principal, UUID resource);

  public Map<UUID, List<Privilege>> listAuthorizations(UUID resource);
}
