package io.unitycatalog.server.service;

import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.control.model.BootstrapStatus;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bootstrap Status Service for Azure AD admin claiming. Reports configuration and current admin
 * state.
 */
public class BootstrapStatusService extends AuthorizedService {

  private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapStatusService.class);
  private final ServerProperties serverProperties;
  private final Repositories repositories;

  public BootstrapStatusService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories.getUserRepository());
    this.repositories = repositories;
    this.serverProperties = serverProperties;
  }

  @Get("/bootstrap-status")
  @ProducesJson
  public BootstrapStatus getBootstrapStatus() {
    boolean bootstrapEnabled =
        Boolean.parseBoolean(serverProperties.getProperty("bootstrap.enabled", "false"));
    boolean hasAzureAdmin = hasAzureAuthenticatedOwner();

    List<String> allowedDomains = parseAllowedDomains();

    LOGGER.debug(
        "Bootstrap status: enabled={}, hasAzureAdmin={}, allowedDomains={}",
        bootstrapEnabled,
        hasAzureAdmin,
        allowedDomains);

    return new BootstrapStatus()
        .bootstrapEnabled(bootstrapEnabled)
        .hasAzureAdmin(hasAzureAdmin)
        .allowedDomains(allowedDomains);
  }

  private boolean hasAzureAuthenticatedOwner() {
    // Check if there's any user with OWNER role that has Azure authentication
    // This is a placeholder - implementation depends on your user/role schema
    return false; // TODO: Implement actual check
  }

  private List<String> parseAllowedDomains() {
    String domains = serverProperties.getProperty("bootstrap.allowedDomains", "");
    return domains.isEmpty() ? List.of() : Arrays.asList(domains.split(","));
  }
}
