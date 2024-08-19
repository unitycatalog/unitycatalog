package io.unitycatalog.server.auth;

import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.casbin.adapter.JDBCAdapter;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.model.Model;

// TODO: This should be JCasbinAuthorizer
public class JCasbinAuthorizer implements UnityCatalogAuthorizer {
  static UserRepository USER_REPOSITORY = UserRepository.getInstance();
  private final Enforcer enforcer;

  public JCasbinAuthorizer() throws Exception {
    Properties properties = HibernateUtils.getHibernateProperties();
    String driver = properties.getProperty("hibernate.connection.driver_class");
    String url = properties.getProperty("hibernate.connection.url");
    String user = properties.getProperty("hibernate.connection.user");
    String password = properties.getProperty("hibernate.connection.password");
    JDBCAdapter adapter = new JDBCAdapter(driver, url, user, password);

    InputStream modelStream = this.getClass().getResourceAsStream("/jcasbin_auth_model.conf");
    String string = IOUtils.toString(modelStream, StandardCharsets.UTF_8);
    Model model = new Model();
    model.loadModelFromText(string);

    enforcer = new Enforcer(model, adapter);
    enforcer.enableAutoSave(true);
  }

  @Override
  public boolean grantAuthorization(UUID principal, UUID resource, Privilege action) {
    return enforcer.addPolicy(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean revokeAuthorization(UUID principal, UUID resource, Privilege action) {
    return enforcer.removePolicy(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean clearAuthorizationsForPrincipal(UUID principal) {
    return enforcer.removeFilteredPolicy(0, principal.toString());
  }

  @Override
  public boolean clearAuthorizationsForResource(UUID resource) {
    return enforcer.removeFilteredPolicy(1, resource.toString());
  }

  @Override
  public boolean addHierarchyChild(UUID parent, UUID child) {
    return enforcer.addNamedGroupingPolicy("g2", parent.toString(), child.toString());
  }

  @Override
  public boolean removeHierarchyChild(UUID parent, UUID child) {
    return enforcer.removeNamedGroupingPolicy("g2", parent.toString(), child.toString());
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
  public boolean authorizeAny(UUID principal, UUID resource, Privilege... actions) {
    return Arrays.stream(actions)
        .anyMatch(
            action ->
                enforcer.enforce(principal.toString(), resource.toString(), action.toString()));
  }

  @Override
  public boolean authorizeAll(UUID principal, UUID resource, Privilege... actions) {
    return Arrays.stream(actions)
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
