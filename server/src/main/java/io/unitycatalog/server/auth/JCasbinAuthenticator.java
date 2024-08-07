package io.unitycatalog.server.auth;

import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.persist.UserRepository;
import org.casbin.jcasbin.main.Enforcer;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class JCasbinAuthenticator implements UnityCatalogAuthenticator {
    private static final UserRepository USER_REPOSITORY = UserRepository.getInstance();
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
    public boolean authorizeAny(UUID principal, UUID resource, List<Privilege> actions) {
        return actions.stream()
                .anyMatch(action ->
                        enforcer.enforce(principal.toString(), resource.toString(), action.toString()));
    }

    @Override
    public boolean authorizeAll(UUID principal, UUID resource, List<Privilege> actions) {
        return actions.stream()
                .allMatch(action ->
                        enforcer.enforce(principal.toString(), resource.toString(), action.toString()));
    }

    @Override
    public List<Privilege> listAuthorizations(UUID principal, UUID resource) {
        return enforcer.getPermissionsForUserInDomain(principal.toString(), resource.toString())
                .stream()
                .map(l -> l.get(1))
                .map(Privilege::valueOf)
                .collect(Collectors.toList());
    }

    @Override
    public Map<UUID, List<Privilege>> listAuthorizations(UUID resource) {
        return USER_REPOSITORY.listUsers().stream()
                .map(user -> UUID.fromString(user.getId()))
                .collect(Collectors.toMap(
                        id -> id,
                        id -> listAuthorizations(id, resource)
                ));
    }
}
