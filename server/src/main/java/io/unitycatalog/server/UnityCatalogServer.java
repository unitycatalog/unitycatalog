package io.unitycatalog.server;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.annotation.JacksonRequestConverterFunction;
import com.linecorp.armeria.server.annotation.JacksonResponseConverterFunction;
import com.linecorp.armeria.server.docs.DocService;
import io.unitycatalog.server.auth.AllowingAuthorizer;
import io.unitycatalog.server.auth.JCasbinAuthorizer;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.UnityAccessDecorator;
import io.unitycatalog.server.auth.decorator.UnityAccessUtil;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.ExceptionHandlingDecorator;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.service.*;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.service.iceberg.FileIOFactory;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.service.iceberg.TableConfigService;
import io.unitycatalog.server.utils.OptionParser;
import io.unitycatalog.server.utils.RESTObjectMapper;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.VersionUtils;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import java.nio.file.Path;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnityCatalogServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnityCatalogServer.class);
  private static final String BASE_PATH = "/api/2.1/unity-catalog/";
  private static final String CONTROL_PATH = "/api/1.0/unity-control/";
  private static final int DEFAULT_PORT = 8080;
  public static final String SERVER_PROPERTIES_FILE = "etc/conf/server.properties";
  private final Server server;
  private final ServerProperties serverProperties;
  private final SecurityContext securityContext;

  static {
    System.setProperty("log4j.configurationFile", "etc/conf/server.log4j2.properties");
    Configurator.initialize(null, "etc/conf/server.log4j2.properties");
  }

  public UnityCatalogServer() {
    this(UnityCatalogServer.builder());
  }

  private UnityCatalogServer(UnityCatalogServer.Builder unityCatalogServerBuilder) {
    setDefaults(unityCatalogServerBuilder);
    Path configurationFolder = Path.of("etc", "conf");
    SecurityConfiguration securityConfiguration = new SecurityConfiguration(configurationFolder);

    this.securityContext =
        new SecurityContext(configurationFolder, securityConfiguration, "server", INTERNAL);
    this.serverProperties = unityCatalogServerBuilder.serverProperties;
    this.server = initializeServer(unityCatalogServerBuilder);
  }

  private void setDefaults(UnityCatalogServer.Builder unityCatalogServerBuilder) {
    if (unityCatalogServerBuilder.port == 0) {
      unityCatalogServerBuilder.port(DEFAULT_PORT);
    }
    if (unityCatalogServerBuilder.serverProperties == null) {
      unityCatalogServerBuilder.serverProperties(new ServerProperties(SERVER_PROPERTIES_FILE));
    }
    if (unityCatalogServerBuilder.credentialOperations == null) {
      AwsCredentialVendor awsCredentialVendor =
          new AwsCredentialVendor(unityCatalogServerBuilder.serverProperties);
      AzureCredentialVendor azureCredentialVendor =
          new AzureCredentialVendor(unityCatalogServerBuilder.serverProperties);
      GcpCredentialVendor gcpCredentialVendor =
          new GcpCredentialVendor(unityCatalogServerBuilder.serverProperties);
      CredentialOperations credentialOperations =
          new CredentialOperations(awsCredentialVendor, azureCredentialVendor, gcpCredentialVendor);
      unityCatalogServerBuilder.credentialOperations(credentialOperations);
    }
  }

  private Server initializeServer(UnityCatalogServer.Builder unityCatalogServerBuilder) {
    ServerBuilder armeriaServerBuilder =
        Server.builder()
            .http(unityCatalogServerBuilder.port)
            .serviceUnder("/docs", new DocService());

    // Init hibernate
    HibernateConfigurator hibernateConfigurator =
        new HibernateConfigurator(unityCatalogServerBuilder.serverProperties);
    // Init all repositories
    Repositories repositories =
        new Repositories(hibernateConfigurator.getSessionFactory(), serverProperties);
    // Init metastore
    repositories.getMetastoreRepository().initMetastoreIfNeeded();
    // Init authorizer
    UnityCatalogAuthorizer authorizer =
        initializeAuthorizer(
            unityCatalogServerBuilder.serverProperties, hibernateConfigurator, repositories);
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
        throw new BaseException(ErrorCode.INTERNAL, "Problem initializing authorizer.");
      }
    } else {
      LOGGER.info("Authorization disabled. Using AllowingAuthorizer.");
      return new AllowingAuthorizer();
    }
  }

  private void addApiServices(
      ServerBuilder armeriaServerBuilder,
      UnityCatalogServer.Builder unityCatalogServerBuilder,
      UnityCatalogAuthorizer authorizer,
      Repositories repositories) {
    LOGGER.info("Adding Unity Catalog API services...");
    CredentialOperations credentialOperations = unityCatalogServerBuilder.credentialOperations;

    // Add support for Unity Catalog APIs
    AuthService authService =
        new AuthService(securityContext, unityCatalogServerBuilder.serverProperties, repositories);
    PermissionService permissionService = new PermissionService(authorizer, repositories);
    Scim2UserService scim2UserService = new Scim2UserService(authorizer, repositories);
    Scim2SelfService scim2SelfService = new Scim2SelfService(authorizer, repositories);
    CatalogService catalogService = new CatalogService(authorizer, repositories);
    SchemaService schemaService = new SchemaService(authorizer, repositories);
    VolumeService volumeService = new VolumeService(authorizer, repositories);
    TableService tableService = new TableService(authorizer, repositories);
    FunctionService functionService = new FunctionService(authorizer, repositories);
    ModelService modelService = new ModelService(authorizer, repositories);
    MetastoreService metastoreService = new MetastoreService(repositories);
    // TODO: combine these into a single service in a follow-up PR
    TemporaryTableCredentialsService temporaryTableCredentialsService =
        new TemporaryTableCredentialsService(authorizer, credentialOperations, repositories);
    TemporaryVolumeCredentialsService temporaryVolumeCredentialsService =
        new TemporaryVolumeCredentialsService(authorizer, credentialOperations, repositories);
    TemporaryModelVersionCredentialsService temporaryModelVersionCredentialsService =
        new TemporaryModelVersionCredentialsService(authorizer, credentialOperations, repositories);
    TemporaryPathCredentialsService temporaryPathCredentialsService =
        new TemporaryPathCredentialsService(credentialOperations);

    JacksonRequestConverterFunction requestConverterFunction =
        new JacksonRequestConverterFunction(
            JsonMapper.builder()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build());
    JacksonResponseConverterFunction scimResponseConverterFunction =
        new JacksonResponseConverterFunction(
            JsonMapper.builder()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                .build());
    armeriaServerBuilder
        .service("/", (ctx, req) -> HttpResponse.of("Hello, Unity Catalog!"))
        .annotatedService(CONTROL_PATH + "auth", authService, requestConverterFunction)
        .annotatedService(
            CONTROL_PATH + "scim2/Users",
            scim2UserService,
            requestConverterFunction,
            scimResponseConverterFunction)
        .annotatedService(
            CONTROL_PATH + "scim2/Me",
            scim2SelfService,
            requestConverterFunction,
            scimResponseConverterFunction)
        .annotatedService(BASE_PATH + "permissions", permissionService)
        .annotatedService(BASE_PATH + "catalogs", catalogService, requestConverterFunction)
        .annotatedService(BASE_PATH + "schemas", schemaService, requestConverterFunction)
        .annotatedService(BASE_PATH + "volumes", volumeService, requestConverterFunction)
        .annotatedService(BASE_PATH + "tables", tableService, requestConverterFunction)
        .annotatedService(BASE_PATH + "functions", functionService, requestConverterFunction)
        .annotatedService(BASE_PATH + "models", modelService, requestConverterFunction)
        .annotatedService(BASE_PATH, metastoreService, requestConverterFunction)
        .annotatedService(
            BASE_PATH + "temporary-table-credentials",
            temporaryTableCredentialsService,
            requestConverterFunction)
        .annotatedService(
            BASE_PATH + "temporary-volume-credentials",
            temporaryVolumeCredentialsService,
            requestConverterFunction)
        .annotatedService(
            BASE_PATH + "temporary-model-version-credentials",
            temporaryModelVersionCredentialsService,
            requestConverterFunction)
        .annotatedService(
            BASE_PATH + "temporary-path-credentials",
            temporaryPathCredentialsService,
            requestConverterFunction);

    addIcebergApiServices(
        armeriaServerBuilder,
        unityCatalogServerBuilder.serverProperties,
        unityCatalogServerBuilder.credentialOperations,
        catalogService,
        schemaService,
        tableService,
        repositories);
  }

  private void addIcebergApiServices(
      ServerBuilder armeriaServerBuilder,
      ServerProperties serverProperties,
      CredentialOperations credentialOperations,
      CatalogService catalogService,
      SchemaService schemaService,
      TableService tableService,
      Repositories repositories) {
    LOGGER.info("Adding Iceberg services...");

    // Add support for Iceberg REST APIs
    ObjectMapper icebergMapper = RESTObjectMapper.mapper();
    JacksonRequestConverterFunction icebergRequestConverter =
        new JacksonRequestConverterFunction(icebergMapper);
    JacksonResponseConverterFunction icebergResponseConverter =
        new JacksonResponseConverterFunction(icebergMapper);
    MetadataService metadataService =
        new MetadataService(new FileIOFactory(credentialOperations, serverProperties));
    TableConfigService tableConfigService =
        new TableConfigService(credentialOperations, serverProperties);

    armeriaServerBuilder.annotatedService(
        BASE_PATH + "iceberg",
        new IcebergRestCatalogService(
            catalogService,
            schemaService,
            tableService,
            tableConfigService,
            metadataService,
            repositories),
        icebergRequestConverter,
        icebergResponseConverter);
  }

  private void addSecurityDecorators(
      ServerBuilder armeriaServerBuilder,
      ServerProperties serverProperties,
      UnityCatalogAuthorizer authorizer,
      Repositories repositories) {
    // TODO: eventually might want to make this secure-by-default.
    if (serverProperties.isAuthorizationEnabled()) {
      LOGGER.info("Enabling security decorators...");

      // Note: Decorators are applied in reverse order.
      UnityAccessDecorator accessDecorator = new UnityAccessDecorator(authorizer, repositories);
      armeriaServerBuilder.routeDecorator().pathPrefix(BASE_PATH).build(accessDecorator);
      armeriaServerBuilder
          .routeDecorator()
          .pathPrefix(CONTROL_PATH)
          .exclude(CONTROL_PATH + "auth/tokens")
          .build(accessDecorator);

      AuthDecorator authDecorator = new AuthDecorator(securityContext, repositories);
      armeriaServerBuilder.routeDecorator().pathPrefix(BASE_PATH).build(authDecorator);
      armeriaServerBuilder
          .routeDecorator()
          .pathPrefix(CONTROL_PATH)
          .exclude(CONTROL_PATH + "auth/tokens")
          .build(authDecorator);

      ExceptionHandlingDecorator exceptionDecorator =
          new ExceptionHandlingDecorator(new GlobalExceptionHandler());
      armeriaServerBuilder.decorator(exceptionDecorator);
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

  public void stop() {
    server.stop().join();
    LOGGER.info("Unity Catalog server stopped.");
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
    private CredentialOperations credentialOperations;

    private Builder() {}

    public UnityCatalogServer.Builder port(int port) {
      this.port = port;
      return this;
    }

    public UnityCatalogServer.Builder serverProperties(ServerProperties serverProperties) {
      this.serverProperties = serverProperties;
      return this;
    }

    public UnityCatalogServer.Builder credentialOperations(
        CredentialOperations credentialOperations) {
      this.credentialOperations = credentialOperations;
      return this;
    }

    public UnityCatalogServer build() {
      return new UnityCatalogServer(this);
    }
  }
}
