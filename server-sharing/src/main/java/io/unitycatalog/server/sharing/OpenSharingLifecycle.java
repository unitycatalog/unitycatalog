package io.unitycatalog.server.sharing;

import static io.unitycatalog.server.model.SecurableType.RECIPIENT;
import static io.unitycatalog.server.model.SecurableType.SHARE;

import io.opensharing.recipient.RecipientStore;
import io.opensharing.runtime.OpenSharing;
import io.opensharing.share.ShareStore;
import io.unitycatalog.server.ArmeriaServerBuilder;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.sharing.catalog.UnityCatalogCatalogConnector;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

/**
 * Builds embedded OpenSharing inside Unity Catalog OSS and mounts its provider API on UC's own
 * Armeria server, behind UC's security decorators.
 */
public final class OpenSharingLifecycle implements AutoCloseable {

  /** The same path the standalone OpenSharing server activates recipients at. */
  private static final String ACTIVATION_PATH = "/api/1.0/opensharing/activations";

  private final SessionFactory sessionFactory;

  private OpenSharingLifecycle(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  /**
   * Mounts OpenSharing's provider API at UC's base path + {@code opensharing/provider}. Recipient
   * activation links point at {@value #ACTIVATION_PATH} on the public URL.
   *
   * <p>OpenSharing's tables, all prefixed {@code os_}, live in UC's database but are mapped by a
   * session factory of their own on UC's connection pool, because UC's session factory is already
   * built. So an operation that touches both sides' tables is two local transactions, not one.
   */
  public static OpenSharingLifecycle start(
      ServerProperties serverProperties,
      ArmeriaServerBuilder server,
      HibernateConfigurator hibernateConfigurator,
      Repositories repositories,
      UnityCatalogAuthorizer authorizer) {
    if (!serverProperties.isAuthorizationEnabled()) {
      throw new IllegalStateException(
          "embedded OpenSharing needs server.authorization=enable: shares and recipients are owned"
              + " by the UC user who creates them");
    }
    String publicUrl = serverProperties.get(Property.OPENSHARING_PUBLIC_URL);
    if (publicUrl == null || publicUrl.isBlank()) {
      throw new IllegalStateException(
          "embedded OpenSharing needs "
              + Property.OPENSHARING_PUBLIC_URL.getKey()
              + ", the address recipients reach this server at");
    }
    String recipientUrl = publicUrl.replaceAll("/+$", "");
    Configuration configuration =
        new Configuration().setProperties(hibernateConfigurator.getSessionFactoryProperties());
    OpenSharing.entities().forEach(configuration::addAnnotatedClass);
    SessionFactory sessionFactory = configuration.buildSessionFactory();
    try {
      HibernateTransactions transactions = new HibernateTransactions(sessionFactory);
      ShareStore shares = new ShareStore(transactions);
      RecipientStore recipients = new RecipientStore(transactions);
      KeyMapper keys = repositories.getKeyMapper();
      keys.registerResolver(SHARE, name -> UUID.fromString(shares.getOrThrow(name).getId()));
      keys.registerResolver(
          RECIPIENT, name -> UUID.fromString(recipients.getOrThrow(name).getId()));
      OpenSharing openSharing =
          OpenSharing.builder()
              .catalog(new UnityCatalogCatalogConnector(repositories, authorizer, serverProperties))
              .transactions(transactions)
              .build();
      server.annotate(
          "opensharing/provider",
          new OpenSharingProviderService(
              openSharing,
              recipientUrl + ACTIVATION_PATH,
              authorizer,
              repositories,
              serverProperties));
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
