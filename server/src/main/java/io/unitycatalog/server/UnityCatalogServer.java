package io.unitycatalog.server;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.linecorp.armeria.server.Server;
import io.unitycatalog.server.auth.AllowingAuthorizer;
import io.unitycatalog.server.auth.JCasbinAuthorizer;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.UnityAccessDecorator;
import io.unitycatalog.server.auth.decorator.UnityAccessUtil;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.BaseExceptionHandler;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.service.AuthDecorator;
import io.unitycatalog.server.service.AuthService;
import io.unitycatalog.server.service.CatalogService;
import io.unitycatalog.server.service.CredentialService;
import io.unitycatalog.server.service.DeltaCommitsService;
import io.unitycatalog.server.service.ExternalLocationService;
import io.unitycatalog.server.service.FunctionService;
import io.unitycatalog.server.service.IcebergRestCatalogService;
import io.unitycatalog.server.service.MetastoreService;
import io.unitycatalog.server.service.ModelService;
import io.unitycatalog.server.service.PermissionService;
import io.unitycatalog.server.service.SchemaService;
import io.unitycatalog.server.service.Scim2SelfService;
import io.unitycatalog.server.service.Scim2UserService;
import io.unitycatalog.server.service.StagingTableService;
import io.unitycatalog.server.service.TableService;
import io.unitycatalog.server.service.TemporaryModelVersionCredentialsService;
import io.unitycatalog.server.service.TemporaryPathCredentialsService;
import io.unitycatalog.server.service.TemporaryTableCredentialsService;
import io.unitycatalog.server.service.TemporaryVolumeCredentialsService;
import io.unitycatalog.server.service.VolumeService;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.service.delta.DeltaApiService;
import io.unitycatalog.server.service.iceberg.FileIOFactory;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.service.iceberg.TableConfigService;
import io.unitycatalog.server.utils.OptionParser;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.VersionUtils;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import java.nio.file.Path;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnityCatalogServer implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnityCatalogServer.class);
  private static final String BASE_PATH = "/api/2.1/unity-catalog/";
  private static final String CONTROL_PATH = "/api/1.0/unity-control/";
  private static final int DEFAULT_PORT = 8080;
  public static final String SERVER_PROPERTIES_FILE = "etc/conf/server.properties";
  private final Server server;
  private final SecurityContext securityContext;
  private final HibernateConfigurator hibernateConfigurator;
  /** True when this server built the configurator itself and must therefore close it. */
  private final boolean ownsHibernateConfigurator;

  static {
    System.setProperty("log4j.configurationFile", "etc/conf/server.log4j2.properties");
    Configurator.initialize(null, "etc/conf/server.log4j2.properties");
  }

  private UnityCatalogServer(UnityCatalogServer.Builder unityCatalogServerBuilder) {
    setDefaults(unityCatalogServerBuilder);
    Path configurationFolder = Path.of("etc", "conf");
    SecurityConfiguration securityConfiguration = new SecurityConfiguration(configurationFolder);

    this.securityContext =
        new SecurityContext(configurationFolder, securityConfiguration, "server", INTERNAL);
    // An injected configurator stays the caller's to close; only a self-built one is ours.
    this.ownsHibernateConfigurator = unityCatalogServerBuilder.hibernateConfigurator == null;
    this.hibernateConfigurator =
        ownsHibernateConfigurator
            ? new HibernateConfigurator(unityCatalogServerBuilder.serverProperties)
            : unityCatalogServerBuilder.hibernateConfigurator;
    try {
      this.server = initializeServer(unityCatalogServerBuilder);
    } catch (Throwable t) {
      // Construction failed after the SessionFactory was built; close it so a failed boot does
      // not leak its connection pool. Errors matter as much as RuntimeExceptions here: a
      // NoClassDefFoundError out of initializeServer() would leak the pool just the same.
      closeOwnedSessionFactory(t);
      throw t;
    }
  }

  /**
   * Closes the SessionFactory if this server created it, leaving an injected one to its owner. A
   * failure to close is attached to {@code primaryFailure} so it cannot mask the original error.
   */
  private void closeOwnedSessionFactory(Throwable primaryFailure) {
    if (!ownsHibernateConfigurator) {
      return;
    }
    try {
      hibernateConfigurator.getSessionFactory().close();
    } catch (Throwable closeFailure) {
      primaryFailure.addSuppressed(closeFailure);
    }
  }

  private void setDefaults(UnityCatalogServer.Builder unityCatalogServerBuilder) {
    if (unityCatalogServerBuilder.port == 0) {
      unityCatalogServerBuilder.port(DEFAULT_PORT);
    }
    if (unityCatalogServerBuilder.serverProperties == null) {
      unityCatalogServerBuilder.serverProperties(new ServerProperties(SERVER_PROPERTIES_FILE));
    }
  }

  private Server initializeServer(UnityCatalogServer.Builder unityCatalogServerBuilder) {
    ArmeriaServerBuilder armeriaServerBuilder =
        new ArmeriaServerBuilder(unityCatalogServerBuilder.port, BASE_PATH, CONTROL_PATH);

    // Init all repositories
    Repositories repositories =
        new Repositories(
            hibernateConfigurator.getSessionFactory(), unityCatalogServerBuilder.serverProperties);
    // Init metastore
    repositories.getMetastoreRepository().initMetastoreIfNeeded();
    // Init authorizer
    UnityCatalogAuthorizer authorizer =
        initializeAuthorizer(
            unityCatalogServerBuilder.serverProperties, hibernateConfigurator, repositories);
    // Configure error response stack traces
    BaseExceptionHandler.setIncludeStackTrace(
        unityCatalogServerBuilder.serverProperties.isIncludeStackTraceInError());
    // Init services
    addApiServices(armeriaServerBuilder, unityCatalogServerBuilder, authorizer, repositories);
    // Init security decorators
    addSecurityDecorators(
        armeriaServerBuilder, unityCatalogServerBuilder.serverProperties, authorizer, repositories);

    return armeriaServerBuilder.build();
  }

  private UnityCatalogAuthorizer initializeAuthorizer(
      ServerProperties serverProperties,
      HibernateConfigurator hibernateConfigurator,
      Repositories repositories) {
    if (serverProperties.isAuthorizationEnabled()) {
      try {
        LOGGER.info("Initializing JCasbinAuthorizer...");
        UnityCatalogAuthorizer authorizer = new JCasbinAuthorizer(hibernateConfigurator);
        new UnityAccessUtil(repositories).initializeAdmin(authorizer);
        return authorizer;
      } catch (Exception e) {
        throw new BaseException(ErrorCode.INTERNAL, "Problem initializing authorizer.", e);
      }
    } else {
      LOGGER.info("Authorization disabled. Using AllowingAuthorizer.");
      return new AllowingAuthorizer();
    }
  }

  private void addApiServices(
      ArmeriaServerBuilder armeriaServerBuilder,
      UnityCatalogServer.Builder unityCatalogServerBuilder,
      UnityCatalogAuthorizer authorizer,
      Repositories repositories) {
    LOGGER.info("Adding Unity Catalog API services...");
    ServerProperties serverProperties = unityCatalogServerBuilder.serverProperties;
    CloudCredentialVendor cloudCredentialVendor =
        unityCatalogServerBuilder.cloudCredentialVendor != null
            ? unityCatalogServerBuilder.cloudCredentialVendor
            : new CloudCredentialVendor(serverProperties);
    StorageCredentialVendor storageCredentialVendor =
        new StorageCredentialVendor(cloudCredentialVendor, repositories.getExternalLocationUtils());

    SchemaService schemaService = new SchemaService(authorizer, repositories, serverProperties);

    // Each annotate* call registers one service. Order is not significant (Armeria routes by path
    // specificity); relative paths are resolved against the protocol's base path ("" mounts at the
    // base path root).
    armeriaServerBuilder
        .annotateAuth("auth", new AuthService(securityContext, serverProperties, repositories))
        .annotateScim("scim2/Users", new Scim2UserService(authorizer, repositories))
        .annotateScim("scim2/Me", new Scim2SelfService(authorizer, repositories))
        .annotateUc("permissions", new PermissionService(authorizer, repositories))
        .annotateUc("catalogs", new CatalogService(authorizer, repositories, serverProperties))
        .annotateUc("schemas", schemaService)
        .annotateUc("volumes", new VolumeService(authorizer, repositories, serverProperties))
        .annotateUc("tables", new TableService(authorizer, repositories, serverProperties))
        .annotateUc(
            "staging-tables", new StagingTableService(authorizer, repositories, serverProperties))
        .annotateUc("functions", new FunctionService(authorizer, repositories, serverProperties))
        .annotateUc("models", new ModelService(authorizer, repositories, serverProperties))
        .annotateUc("", new MetastoreService(repositories))
        .annotateUc(
            "temporary-table-credentials",
            new TemporaryTableCredentialsService(
                storageCredentialVendor, repositories, serverProperties))
        .annotateUc(
            "temporary-volume-credentials",
            new TemporaryVolumeCredentialsService(storageCredentialVendor, repositories))
        .annotateUc(
            "temporary-model-version-credentials",
            new TemporaryModelVersionCredentialsService(storageCredentialVendor, repositories))
        .annotateUc(
            "temporary-path-credentials",
            new TemporaryPathCredentialsService(storageCredentialVendor))
        .annotateUc(
            "credentials", new CredentialService(authorizer, repositories, serverProperties))
        .annotateUc(
            "delta/preview/commits",
            new DeltaCommitsService(authorizer, repositories, serverProperties))
        .annotateUc(
            "external-locations",
            new ExternalLocationService(authorizer, repositories, serverProperties));
    addIcebergApiServices(
        armeriaServerBuilder,
        serverProperties,
        storageCredentialVendor,
        schemaService,
        repositories);
    addDeltaApiServices(
        armeriaServerBuilder, authorizer, repositories, serverProperties, storageCredentialVendor);
  }

  private void addIcebergApiServices(
      ArmeriaServerBuilder armeriaServerBuilder,
      ServerProperties serverProperties,
      StorageCredentialVendor storageCredentialVendor,
      SchemaService schemaService,
      Repositories repositories) {
    LOGGER.info("Adding Iceberg services...");

    // Add support for Iceberg REST APIs
    MetadataService metadataService =
        new MetadataService(new FileIOFactory(storageCredentialVendor, serverProperties));
    TableConfigService tableConfigService =
        new TableConfigService(storageCredentialVendor, serverProperties);

    armeriaServerBuilder.annotateIceberg(
        "iceberg",
        new IcebergRestCatalogService(
            schemaService, tableConfigService, metadataService, repositories));
  }

  private void addDeltaApiServices(
      ArmeriaServerBuilder armeriaServerBuilder,
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties,
      StorageCredentialVendor storageCredentialVendor) {
    LOGGER.info("Adding UC Delta API services...");
    DeltaApiService deltaApiService =
        new DeltaApiService(authorizer, repositories, serverProperties, storageCredentialVendor);
    armeriaServerBuilder.annotateDelta("", deltaApiService);
  }

  private void addSecurityDecorators(
      ArmeriaServerBuilder armeriaServerBuilder,
      ServerProperties serverProperties,
      UnityCatalogAuthorizer authorizer,
      Repositories repositories) {
    // TODO: eventually might want to make this secure-by-default.
    if (serverProperties.isAuthorizationEnabled()) {
      LOGGER.info("Enabling security decorators...");
      armeriaServerBuilder.withSecurityDecorators(
          new UnityAccessDecorator(authorizer, repositories),
          new AuthDecorator(securityContext, repositories));
    }
  }

  public static void main(String[] args) {
    OptionParser options = new OptionParser();
    options.parse(args);
    // Start Unity Catalog server
    UnityCatalogServer unityCatalogServer =
        UnityCatalogServer.builder().port(options.getPort() + 1).build();
    unityCatalogServer.printArt();
    unityCatalogServer.start();
    // Start URL transcoder
    Vertx vertx = Vertx.vertx();
    Verticle transcodeVerticle =
        new URLTranscoderVerticle(options.getPort(), options.getPort() + 1);
    vertx.deployVerticle(transcodeVerticle);
  }

  public void start() {
    LOGGER.info("Starting Unity Catalog server...");
    server.start().join();
    LOGGER.info("Unity Catalog server started.");
  }

  /** Stops the HTTP server. The server can be restarted afterwards with {@link #start()}. */
  public void stop() {
    server.stop().join();
    LOGGER.info("Unity Catalog server stopped.");
  }

  /**
   * Stops the server and closes the Hibernate SessionFactory it created, releasing its pooled
   * database connections, which the Armeria shutdown does not touch and which would otherwise stay
   * open until the JVM exits. A configurator supplied via {@link Builder#hibernateConfigurator} is
   * left open — the caller owns its lifecycle. Unlike {@link #stop()}, a server that owns its
   * SessionFactory must not be restarted after this call: the factory is closed, so all persistence
   * operations would fail. Safe to call more than once and safe to call before {@link #start()}.
   */
  @Override
  public void close() {
    try {
      stop();
    } finally {
      if (ownsHibernateConfigurator) {
        hibernateConfigurator.getSessionFactory().close();
      }
    }
  }

  private void printArt() {
    String art =
        "################################################################### \n"
            + "#  _    _       _ _            _____      _        _              #\n"
            + "# | |  | |     (_) |          / ____|    | |      | |             #\n"
            + "# | |  | |_ __  _| |_ _   _  | |     __ _| |_ __ _| | ___   __ _  #\n"
            + "# | |  | | '_ \\| | __| | | | | |    / _` | __/ _` | |/ _ \\ / _` | #\n"
            + "# | |__| | | | | | |_| |_| | | |___| (_| | || (_| | | (_) | (_| | #\n"
            + "#  \\____/|_| |_|_|\\__|\\__, |  \\_____\\__,_|\\__\\__,_|_|\\___/ \\__, | #\n"
            + "#                      __/ |                                __/ | #\n"
            + "#                     |___/               "
            + String.format("%15s", ("v" + VersionUtils.VERSION))
            + "  |___/  #\n"
            + "###################################################################\n";
    System.out.println(art);
  }

  public static UnityCatalogServer.Builder builder() {
    return new UnityCatalogServer.Builder();
  }

  public static class Builder {
    private int port;
    private ServerProperties serverProperties;
    private HibernateConfigurator hibernateConfigurator;
    private CloudCredentialVendor cloudCredentialVendor;

    private Builder() {}

    public UnityCatalogServer.Builder port(int port) {
      this.port = port;
      return this;
    }

    public UnityCatalogServer.Builder serverProperties(ServerProperties serverProperties) {
      this.serverProperties = serverProperties;
      return this;
    }

    /**
     * Uses the given {@link HibernateConfigurator} instead of creating one from the server
     * properties. Lets tests share the server's session factory and customize the hibernate
     * properties (e.g. run against PostgreSQL via Testcontainers). The server never closes this
     * factory, so the caller owns its lifecycle.
     */
    public UnityCatalogServer.Builder hibernateConfigurator(
        HibernateConfigurator hibernateConfigurator) {
      this.hibernateConfigurator = hibernateConfigurator;
      return this;
    }

    public UnityCatalogServer.Builder credentialOperations(
        CloudCredentialVendor cloudCredentialVendor) {
      this.cloudCredentialVendor = cloudCredentialVendor;
      return this;
    }

    public UnityCatalogServer build() {
      return new UnityCatalogServer(this);
    }
  }
}
