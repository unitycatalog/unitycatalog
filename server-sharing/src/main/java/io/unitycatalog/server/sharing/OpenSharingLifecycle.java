package io.unitycatalog.server.sharing;

import io.opensharing.catalog.unity.UnityCatalogConnector;
import io.opensharing.runtime.OpenSharing;
import io.unitycatalog.server.auth.JCasbinAuthorizer;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts and stops the embedded OpenSharing Spring context inside Unity Catalog OSS. */
public final class OpenSharingLifecycle implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenSharingLifecycle.class);

  private final AutoCloseable context;

  private OpenSharingLifecycle(AutoCloseable context) {
    this.context = context;
  }

  /** Timeouts for the loopback call to UC's own Armeria server — see {@link #start}. */
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);

  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

  /**
   * Starts embedded OpenSharing against UC's own database — the same JDBC connection {@code
   * hibernateConfigurator} already opened for UC's own tables, not a second one. Safe within one
   * JVM (H2 shares one in-memory Database instance per canonical file path per process; a real
   * database server is designed for exactly this), and none of OpenSharing's table names (all
   * prefixed {@code os_}) collide with UC's ({@code uc_}-prefixed) ones. There is no coordinated
   * transaction between the two: each keeps its own connection pool and Hibernate/JPA session, so
   * a single logical operation touching both sides' tables is still two local transactions, not
   * one atomic unit.
   *
   * <p>Bound to {@code 127.0.0.1} rather than every interface: its port is reached only through
   * {@code URLTranscoderVerticle}'s path-based routing on UC's own public port, which is how a
   * client sees one address for the whole process rather than a second server to know about and
   * open a second port for. A client that guessed the internal port and reached it directly would
   * still work — nothing about the routing is a security boundary between UC and OpenSharing, both
   * of which trust each other completely in one process — but restricting the bind address is what
   * makes "one address" true on the network, not merely in how the demo happens to be run.
   *
   * <p>Catalog access is OpenSharing's own {@code UnityCatalogConnector} (the same class standalone
   * mode uses) pointed at UC's Armeria server on {@code 127.0.0.1:armeriaPort} — a real HTTP call,
   * not a direct repository read. That is deliberate: UC enforces its own grants (metastore /
   * catalog / schema / table privileges) in {@code UnityAccessDecorator}, a decorator wrapped
   * around Armeria's HTTP dispatch, not inside the repositories or service methods themselves —
   * calling a repository directly, or even calling a {@code TableService} method as a plain Java
   * call, bypasses every grant check UC has. Going through the real HTTP endpoint, on the same
   * loopback address UC itself is reached on, is what lets this be embedded without becoming a
   * second implementation of UC's authorization policy to keep in sync by hand.
   */
  public static OpenSharingLifecycle start(
      ServerProperties serverProperties,
      SecurityContext securityContext,
      Repositories repositories,
      HibernateConfigurator hibernateConfigurator,
      int armeriaPort) {
    if (!serverProperties.isOpenSharingEnabled()) {
      return null;
    }
    // main() always starts Armeria on the public port + 1 (see UnityCatalogServer.main), so the
    // public address a client — and a recipient's activation URL / config.share — actually
    // reaches this same process on is one below armeriaPort. Derived rather than a separate
    // config property, so there is nothing for an operator to keep in sync with the real port.
    String externalBaseUrl = "http://localhost:" + (armeriaPort - 1);
    LOGGER.info(
        "Starting embedded OpenSharing on {} (internal port {}), routed from UC's own port under"
            + " {}",
        externalBaseUrl,
        serverProperties.getOpenSharingPort(),
        serverProperties.getOpenSharingRoutedPathPrefixes());
    try {
      URI ucLoopback = URI.create("http://127.0.0.1:" + armeriaPort + "/api/2.1/unity-catalog");
      OpenSharing.EmbeddedBuilder builder =
          OpenSharing.embedded()
              .catalog(new UnityCatalogConnector(ucLoopback, CONNECT_TIMEOUT, REQUEST_TIMEOUT))
              .identityResolver(
                  new UnityCatalogProviderIdentityResolver(
                      serverProperties, securityContext, repositories))
              .property("server.port", serverProperties.getOpenSharingPort())
              .property("server.address", "127.0.0.1")
              .property(
                  "opensharing.protocol-prefix", serverProperties.getOpenSharingProtocolPrefix())
              .property(
                  "opensharing.provider.base-path",
                  serverProperties.getOpenSharingProviderBasePath())
              .property(
                  "opensharing.activation.base-path",
                  serverProperties.getOpenSharingActivationBasePath())
              .property("opensharing.activation.external-base-url", externalBaseUrl)
              .property(
                  "opensharing.security.credential-encryption-key",
                  serverProperties.getOpenSharingCredentialEncryptionKey());
      applyUcDataSource(builder, hibernateConfigurator.getHibernateProperties());
      AutoCloseable context = builder.run();
      LOGGER.info("Embedded OpenSharing started");
      return new OpenSharingLifecycle(context);
    } catch (Exception e) {
      throw new IllegalStateException("failed to start embedded OpenSharing", e);
    }
  }

  /**
   * Maps UC's own {@code hibernate.connection.*} settings to the {@code spring.datasource.*} ones
   * OpenSharing's JPA layer reads, so it opens connections against UC's own database instead of a
   * datasource of its own. Whatever UC connects to — H2 file, Postgres, MySQL — OpenSharing
   * follows.
   */
  private static void applyUcDataSource(OpenSharing.EmbeddedBuilder builder, Properties hibernate) {
    String url = hibernate.getProperty("hibernate.connection.url");
    if (url == null || url.isBlank()) {
      throw new IllegalStateException(
          "embedded OpenSharing could not determine UC's database: "
              + "hibernate.connection.url is not set in etc/conf/hibernate.properties");
    }
    builder.property("spring.datasource.url", url);
    String driver = hibernate.getProperty("hibernate.connection.driver_class");
    if (driver != null && !driver.isBlank()) {
      builder.property("spring.datasource.driver-class-name", driver);
    }
    // UC's own connection decides the credentials these two connection pools must agree on to open
    // the same database — not OpenSharing's application.yml default of "sa". If UC's
    // hibernate.properties leaves username unset, UC connected (and, on first connect, H2 created
    // the database's admin user) with an empty one, so this sets an explicit empty string here too
    // rather than falling through to OpenSharing's own default. Username is resolved the same way
    // JCasbinAuthorizer resolves it for its own JDBC adapter: UC's deployment docs and Helm chart
    // set the non-standard hibernate.connection.user, which Hibernate's own connection provider
    // (and, until now, this method) silently ignores in favor of the standard
    // hibernate.connection.username — so a Postgres/MySQL setup that followed those docs literally
    // would otherwise connect with no username at all.
    String username = JCasbinAuthorizer.resolveConnectionUsername(hibernate);
    builder.property("spring.datasource.username", username == null ? "" : username);
    builder.property(
        "spring.datasource.password", hibernate.getProperty("hibernate.connection.password", ""));
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
