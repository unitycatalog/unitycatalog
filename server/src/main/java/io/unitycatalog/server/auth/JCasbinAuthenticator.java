package io.unitycatalog.server.auth;

import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.persist.UserRepository;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.casbin.jcasbin.main.Enforcer;

public class JCasbinAuthenticator implements UnityCatalogAuthenticator {
  static UserRepository USER_REPOSITORY = UserRepository.getInstance();
  Enforcer enforcer = new Enforcer("src/main/resources/jcasbin_auth_model.conf");

  @Override
  public boolean grantAuthorization(UUID principal, UUID resource, Privilege action) {
    return enforcer.addPolicy(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean revokeAuthorization(UUID principal, UUID resource, Privilege action) {
    return enforcer.removePolicy(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean clearAuthorizations(UUID principal) {
    return enforcer.removeFilteredPolicy(0, principal.toString());
  }

  @Override
  public boolean addHierarchyChild(UUID parent, UUID child) {
    return enforcer.addNamedGroupingPolicy("g2", child.toString(), parent.toString());
  }

  @Override
  public boolean removeHierarchyChild(UUID parent, UUID child) {
    return enforcer.removeNamedGroupingPolicy("g2", child.toString(), parent.toString());
  }

  @Override
  public boolean removeHierarchyChildren(UUID resource) {
    return enforcer.removeFilteredNamedGroupingPolicy("g2", 0, resource.toString());
  }

  @Override
  public boolean authorize(UUID principal, UUID resource, Privilege action) {
    return enforcer.enforce(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean authorizeAny(UUID principal, UUID resource, List<Privilege> actions) {
    return actions.stream()
        .anyMatch(
            action ->
                enforcer.enforce(principal.toString(), resource.toString(), action.toString()));
  }

  @Override
  public boolean authorizeAll(UUID principal, UUID resource, List<Privilege> actions) {
    return actions.stream()
        .allMatch(
            action ->
                enforcer.enforce(principal.toString(), resource.toString(), action.toString()));
  }

  @Override
  public List<Privilege> listAuthorizations(UUID principal, UUID resource) {
    List<List<String>> list =
        enforcer.getPermissionsForUserInDomain(principal.toString(), resource.toString());
    return list.stream().map(l -> l.get(2)).map(Privilege::fromValue).collect(Collectors.toList());
  }

  @Override
  public Map<UUID, List<Privilege>> listAuthorizations(UUID resource) {
    Map<UUID, List<Privilege>> result =
        USER_REPOSITORY.listUsers().stream()
            .map(user -> UUID.fromString(user.getId()))
            .collect(Collectors.toMap(id -> id, id -> listAuthorizations(id, resource)));
    // Remove users with no authorizations
    result.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    return result;
  }
}
