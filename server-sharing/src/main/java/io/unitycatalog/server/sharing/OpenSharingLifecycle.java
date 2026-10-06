package io.unitycatalog.server.sharing;

import io.opensharing.runtime.OpenSharing;
import io.opensharing.share.ShareEntity;
import io.unitycatalog.server.ArmeriaServerBuilder;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.sharing.catalog.UnityCatalogCatalogConnector;
import io.unitycatalog.server.utils.ServerProperties;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

/**
 * Builds embedded OpenSharing inside Unity Catalog OSS and mounts its routes on UC's own Armeria
 * server, behind UC's security decorators.
 */
public final class OpenSharingLifecycle implements AutoCloseable {

  private final SessionFactory sessionFactory;

  private OpenSharingLifecycle(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  /**
   * Mounts OpenSharing's provider API at UC's base path + {@code opensharing/provider}.
   *
   * <p>OpenSharing's tables, all prefixed {@code os_}, live in UC's database but are mapped by a
   * session factory of their own, because UC's is already built. So an operation that touches both
   * sides' tables is two local transactions, not one.
   */
  public static OpenSharingLifecycle start(
      ServerProperties serverProperties,
      ArmeriaServerBuilder server,
      HibernateConfigurator hibernateConfigurator,
      Repositories repositories,
      UnityCatalogAuthorizer authorizer) {
    if (!serverProperties.isAuthorizationEnabled()) {
      throw new IllegalStateException(
          "embedded OpenSharing needs server.authorization=enable: shares are owned by the UC user"
              + " who creates them");
    }
    SessionFactory sessionFactory =
        new Configuration()
            .setProperties(hibernateConfigurator.getHibernateProperties())
            .addAnnotatedClass(ShareEntity.class)
            .buildSessionFactory();
    try {
      OpenSharing openSharing =
          OpenSharing.builder()
              .catalog(new UnityCatalogCatalogConnector(repositories, authorizer))
              .transactions(new HibernateTransactions(sessionFactory))
              .build();
      server.annotate(
          "opensharing/provider",
          new OpenSharingShareService(openSharing, repositories.getUserRepository()));
      return new OpenSharingLifecycle(sessionFactory);
    } catch (RuntimeException e) {
      sessionFactory.close();
      throw e;
    }
  }

  @Override
  public void close() {
    sessionFactory.close();
  }
}
