package io.unitycatalog.server.service.sharing;

import com.linecorp.armeria.server.VirtualHostBuilder;
import io.opensharing.protocol.OpenSharingRecipientApi;
import io.opensharing.runtime.OpenSharing;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;

/**
 * Builds embedded OpenSharing inside Unity Catalog OSS. {@code UnityCatalogServer} registers what
 * it builds with the server: the provider API behind UC's security decorators, and the
 * recipient-facing API, which authenticates recipients by their own tokens, outside them.
 *
 * <p>OpenSharing's tables, all prefixed {@code os_}, are mapped by UC's own session factory, so
 * OpenSharing and UC share one connection pool and an operation that touches both sides' tables is
 * one transaction.
 */
public final class OpenSharingLifecycle {

  private final OpenSharing openSharing;
  private final OpenSharingProviderService providerService;

  private OpenSharingLifecycle(
      OpenSharing openSharing, OpenSharingProviderService providerService) {
    this.openSharing = openSharing;
    this.providerService = providerService;
  }

  /**
   * Builds embedded OpenSharing on UC's database and authorizer. It keeps OpenSharing's default
   * protocol and activation paths, the standalone server's, so recipients see one layout.
   *
   * @throws IllegalStateException when the server cannot host it as configured
   */
  public static OpenSharingLifecycle start(
      ServerProperties serverProperties,
      Repositories repositories,
      UnityCatalogAuthorizer authorizer) {
    if (!serverProperties.isAuthorizationEnabled()) {
      throw new IllegalStateException(
          "embedded OpenSharing needs server.authorization=enable: shares are owned by the UC user"
              + " who creates them");
    }
    String publicUrl = serverProperties.get(Property.OPENSHARING_PUBLIC_URL);
    if (publicUrl == null || publicUrl.isBlank()) {
      throw new IllegalStateException(
          "embedded OpenSharing needs "
              + Property.OPENSHARING_PUBLIC_URL.getKey()
              + ", the address recipients reach this server at");
    }
    HibernateTransactions transactions =
        new HibernateTransactions(repositories.getSessionFactory());
    OpenSharing openSharing =
        OpenSharing.builder()
            .catalog(new UnityCatalogCatalogConnector(repositories, authorizer, serverProperties))
            .publicUrl(publicUrl)
            .transactions(transactions)
            .build();
    return new OpenSharingLifecycle(
        openSharing,
        new OpenSharingProviderService(
            openSharing, transactions, authorizer, repositories, serverProperties));
  }

  /** OpenSharing's provider API, to be mounted at {@code opensharing/provider}. */
  public OpenSharingProviderService providerService() {
    return providerService;
  }

  /**
   * Mounts the sharing protocol and recipient activation on {@code host}, the virtual host that
   * serves UC's API. They bring their own JSON converters and error handling.
   */
  public void mountRecipientApi(VirtualHostBuilder host) {
    OpenSharingRecipientApi.mount(host, openSharing);
  }
}
