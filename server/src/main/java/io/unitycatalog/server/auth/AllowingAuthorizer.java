package io.unitycatalog.server.auth;

import io.unitycatalog.server.persist.model.Privileges;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An authorizer that allows all actions.
 *
 * <p>This is a simple implementation of UnityCatalogAuthorizer that allows all actions for all
 * principals. This is useful for testing and development purposes. It does not enforce any access
 * control policies nor persist any data.
 */
public class AllowingAuthorizer implements UnityCatalogAuthorizer {
  @Override
  public boolean grantAuthorization(UUID principal, UUID resource, Privileges action) {
    return true;
  }

  @Override
  public boolean revokeAuthorization(UUID principal, UUID resource, Privileges action) {
    return true;
  }

  @Override
  public boolean clearAuthorizationsForPrincipal(UUID principal) {
    return true;
  }

  @Override
  public boolean clearAuthorizationsForResource(UUID resource) {
    return true;
  }

  @Override
  public boolean addHierarchyChild(UUID parent, UUID child) {
    return true;
  }

  @Override
  public boolean removeHierarchyChild(UUID parent, UUID child) {
    return true;
  }

  @Override
  public boolean removeHierarchyChildren(UUID resource) {
    return true;
  }

  @Override
  public UUID getHierarchyParent(UUID resource) {
    return null;
  }

  @Override
  public boolean authorize(UUID principal, UUID resource, Privileges action) {
    return true;
  }

  @Override
  public boolean authorizeAny(UUID principal, UUID resource, Privileges... actions) {
    return true;
  }

  @Override
  public boolean authorizeAll(UUID principal, UUID resource, Privileges... actions) {
    return true;
  }

  @Override
  public List<Privileges> listAuthorizations(UUID principal, UUID resource) {
    return List.of();
  }

  @Override
  public Map<UUID, List<Privileges>> listAuthorizations(UUID resource) {
    return Map.of();
  }
}
