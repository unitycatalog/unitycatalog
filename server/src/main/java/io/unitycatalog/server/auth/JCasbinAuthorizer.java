package io.unitycatalog.server.auth;

import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
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

/**
 * An authorizer that uses the JCasbin library to enforce access control policies.
 *
 * <p>This class is an implementation of UnityCatalogAuthorizor that uses JCasbin as the back end to
 * both store and enforce access control policies.
 *
 * <p>The implementation stores the policies in a database using the JDBCAdapter class.
 */
public class JCasbinAuthorizer implements UnityCatalogAuthorizer {
  private final Enforcer enforcer;

  private static final int PRINCIPAL_INDEX = 0;
  private static final int RESOURCE_INDEX = 1;
  private static final int PRIVILEGE_INDEX = 2;

  private static final String HIERARCHY_POLICY = "g2";
  private static final int HIERARCHY_PARENT_INDEX = 0;
  private static final int HIERARCHY_CHILD_INDEX = 1;

  public JCasbinAuthorizer(HibernateConfigurator hibernateConfigurator) throws Exception {
    Properties properties = hibernateConfigurator.getHibernateProperties();
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
  public boolean grantAuthorization(UUID principal, UUID resource, Privileges action) {
    return enforcer.addPolicy(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean revokeAuthorization(UUID principal, UUID resource, Privileges action) {
    return enforcer.removePolicy(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean clearAuthorizationsForPrincipal(UUID principal) {
    return enforcer.removeFilteredPolicy(PRINCIPAL_INDEX, principal.toString());
  }

  @Override
  public boolean clearAuthorizationsForResource(UUID resource) {
    return enforcer.removeFilteredPolicy(RESOURCE_INDEX, resource.toString());
  }

  @Override
  public boolean addHierarchyChild(UUID parent, UUID child) {
    return enforcer.addNamedGroupingPolicy(HIERARCHY_POLICY, parent.toString(), child.toString());
  }

  @Override
  public boolean removeHierarchyChild(UUID parent, UUID child) {
    return enforcer.removeNamedGroupingPolicy(
        HIERARCHY_POLICY, parent.toString(), child.toString());
  }

  @Override
  public boolean removeHierarchyChildren(UUID resource) {
    return enforcer.removeFilteredNamedGroupingPolicy(
        HIERARCHY_POLICY, HIERARCHY_PARENT_INDEX, resource.toString());
  }

  @Override
  public UUID getHierarchyParent(UUID resource) {
    List<List<String>> policy =
        enforcer.getFilteredNamedGroupingPolicy(
            HIERARCHY_POLICY, HIERARCHY_CHILD_INDEX, resource.toString());
    if (policy.isEmpty() || policy.get(0).isEmpty()) {
      return null;
    }
    return UUID.fromString(policy.get(0).get(HIERARCHY_PARENT_INDEX));
  }

  @Override
  public boolean authorize(UUID principal, UUID resource, Privileges action) {
    return enforcer.enforce(principal.toString(), resource.toString(), action.toString());
  }

  @Override
  public boolean authorizeAny(UUID principal, UUID resource, Privileges... actions) {
    return Arrays.stream(actions)
        .anyMatch(
            action ->
                enforcer.enforce(principal.toString(), resource.toString(), action.toString()));
  }

  @Override
  public boolean authorizeAll(UUID principal, UUID resource, Privileges... actions) {
    return Arrays.stream(actions)
        .allMatch(
            action ->
                enforcer.enforce(principal.toString(), resource.toString(), action.toString()));
  }

  @Override
  public List<Privileges> listAuthorizations(UUID principal, UUID resource) {
    List<List<String>> list =
        enforcer.getPermissionsForUserInDomain(principal.toString(), resource.toString());
    return list.stream()
        .map(l -> l.get(PRIVILEGE_INDEX))
        .map(Privileges::fromValue)
        .collect(Collectors.toList());
  }

  @Override
  public Map<UUID, List<Privileges>> listAuthorizations(UUID resource) {
    return enforcer.getFilteredPolicy(RESOURCE_INDEX, resource.toString()).stream()
        .collect(
            Collectors.groupingBy(
                l -> UUID.fromString(l.get(PRINCIPAL_INDEX)),
                Collectors.mapping(
                    l -> Privileges.fromValue(l.get(PRIVILEGE_INDEX)), Collectors.toList())));
  }
}
