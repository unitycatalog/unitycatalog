package io.unitycatalog.server.auth;

import io.unitycatalog.server.model.Privilege;
import org.casbin.jcasbin.main.Enforcer;

import java.util.List;
import java.util.UUID;

public class JCasbinAuthenticator implements UnityCatalogAuthenticator {
    Enforcer enforcer = new Enforcer("src/main/resources/jcasbin_auth_model.conf");

    @Override
    public void grantAuthorization(UUID principal, UUID resource, Privilege action) {
        enforcer.addPolicy(principal.toString(), resource.toString(), action.toString());
    }

    @Override
    public void revokeAuthorization(UUID principal, UUID resource, Privilege action) {
        enforcer.removePolicy(principal.toString(), resource.toString(), action.toString());
    }

    @Override
    public void clearAuthorizations(UUID resource) {
        enforcer.removeFilteredPolicy(0, resource.toString());
    }

    @Override
    public void addHierarchyChild(UUID parent, UUID child) {
        enforcer.addNamedGroupingPolicy("g2", parent.toString(), child.toString());
    }

    @Override
    public void removeHierarchyChild(UUID parent, UUID child) {
        enforcer.removeNamedGroupingPolicy("g2", parent.toString(), child.toString());
    }

    @Override
    public void removeHierarchyChildren(UUID resource) {
        enforcer.removeFilteredNamedGroupingPolicy("g2", 0, resource.toString());
    }

    @Override
    public boolean authorize(UUID principal, UUID resource, Privilege action) {
        return enforcer.enforce(principal.toString(), resource.toString(), action.toString());
    }

    @Override
    public boolean authorizeAny(UUID privilege, UUID resource, List<Privilege> actions) {
        return actions.stream()
                .anyMatch(action ->
                        enforcer.enforce(privilege.toString(), resource.toString(), action.toString()));
    }

    @Override
    public boolean authorizeAll(UUID privilege, UUID resource, List<Privilege> actions) {
        return actions.stream()
                .allMatch(action ->
                        enforcer.enforce(privilege.toString(), resource.toString(), action.toString()));
    }
}
