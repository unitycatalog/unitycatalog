package io.unitycatalog.server.auth;

import io.unitycatalog.server.model.Privilege;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface UnityCatalogAuthenticator {
    public void grantAuthorization(UUID principal, UUID resource, Privilege action);

    public void revokeAuthorization(UUID principal, UUID resource, Privilege action);

    public void clearAuthorizations(UUID resource);

    public void addHierarchyChild(UUID parent, UUID child);

    public void removeHierarchyChild(UUID parent, UUID child);

    public void removeHierarchyChildren(UUID resource);

    public boolean authorize(UUID principal, UUID resource, Privilege action);

    public boolean authorizeAny(UUID principal, UUID resource, List<Privilege> actions);

    public boolean authorizeAll(UUID principal, UUID resource, List<Privilege> actions);

    public List<Privilege> listAuthorizations(UUID principal, UUID resource);

    public Map<UUID, List<Privilege>> listAuthorizations(UUID resource);
}
