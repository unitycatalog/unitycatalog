package io.unitycatalog.server.sharing;

import io.opensharing.runtime.OpenSharing;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.ServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts and stops the embedded OpenSharing Spring context inside Unity Catalog OSS. */
public final class OpenSharingLifecycle implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenSharingLifecycle.class);

  private final AutoCloseable context;

  private OpenSharingLifecycle(AutoCloseable context) {
    this.context = context;
  }

  public static OpenSharingLifecycle start(
      ServerProperties serverProperties,
      SecurityContext securityContext,
      Repositories repositories) {
    if (!serverProperties.isOpenSharingEnabled()) {
      return null;
    }
    LOGGER.info(
        "Starting embedded OpenSharing on port {} with protocol prefix {}",
        serverProperties.getOpenSharingPort(),
        serverProperties.getOpenSharingProtocolPrefix());
    try {
      AutoCloseable context =
          OpenSharing.embedded()
              .catalog(new UnityCatalogEmbeddedConnector(repositories))
              .identityResolver(
                  new UnityCatalogProviderIdentityResolver(
                      serverProperties, securityContext, repositories))
              .property("server.port", serverProperties.getOpenSharingPort())
              .property(
                  "opensharing.protocol-prefix", serverProperties.getOpenSharingProtocolPrefix())
              .property(
                  "opensharing.activation.external-base-url",
                  serverProperties.getOpenSharingExternalBaseUrl())
              .property("spring.datasource.url", serverProperties.getOpenSharingDatasourceUrl())
              .property(
                  "opensharing.security.credential-encryption-key",
                  serverProperties.getOpenSharingCredentialEncryptionKey())
              .run();
      LOGGER.info("Embedded OpenSharing started");
      return new OpenSharingLifecycle(context);
    } catch (Exception e) {
      throw new IllegalStateException("failed to start embedded OpenSharing", e);
    }
  }

  @Override
  public void close() {
    if (context != null) {
      try {
        LOGGER.info("Stopping embedded OpenSharing");
        context.close();
      } catch (Exception e) {
        throw new IllegalStateException("failed to stop embedded OpenSharing", e);
      }
    }
  }
}
