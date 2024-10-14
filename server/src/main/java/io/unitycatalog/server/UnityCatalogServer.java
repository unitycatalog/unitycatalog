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
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.service.*;
import io.unitycatalog.server.service.credential.CredentialOperations;
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

  private SecurityConfiguration securityConfiguration;
  private SecurityContext securityContext;

  static {
    System.setProperty("log4j.configurationFile", "etc/conf/server.log4j2.properties");
    Configurator.initialize(null, "etc/conf/server.log4j2.properties");
  }

  Server server;
  private static final String basePath = "/api/2.1/unity-catalog/";
  private static final String controlPath = "/api/1.0/unity-control/";

  public UnityCatalogServer() {
    new UnityCatalogServer(8080);
  }

  public UnityCatalogServer(int port) {

    Path configurationFolder = Path.of("etc", "conf");

    securityConfiguration = new SecurityConfiguration(configurationFolder);
    securityContext =
        new SecurityContext(configurationFolder, securityConfiguration, "server", INTERNAL);

    ServerBuilder sb = Server.builder().serviceUnder("/docs", new DocService()).http(port);
    addServices(sb);

    server = sb.build();
  }

  private void addServices(ServerBuilder sb) {
    ObjectMapper unityMapper =
        JsonMapper.builder().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    JacksonRequestConverterFunction unityConverterFunction =
        new JacksonRequestConverterFunction(unityMapper);

    ObjectMapper responseMapper =
        JsonMapper.builder()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .serializationInclusion(JsonInclude.Include.NON_NULL)
            .build();
    JacksonResponseConverterFunction scimResponseFunction =
        new JacksonResponseConverterFunction(responseMapper);

    // Credentials Service
    CredentialOperations credentialOperations = new CredentialOperations();

    ServerProperties serverProperties = ServerProperties.getInstance();
    boolean authorizationEnabled = serverProperties.isAuthorizationEnabled();

    UnityCatalogAuthorizer authorizer = null;
    try {
      if (authorizationEnabled) {
        authorizer = new JCasbinAuthorizer();
        UnityAccessUtil.initializeAdmin(authorizer);
      } else {
        authorizer = new AllowingAuthorizer();
      }
    } catch (Exception e) {
      throw new BaseException(ErrorCode.INTERNAL, "Problem initializing authorizer.");
    }

    // Add support for Unity Catalog APIs
    AuthService authService = new AuthService(securityContext);
    PermissionService permissionService = new PermissionService(authorizer);
    Scim2UserService scim2UserService = new Scim2UserService(authorizer);
    CatalogService catalogService = new CatalogService(authorizer);
    SchemaService schemaService = new SchemaService(authorizer);
    VolumeService volumeService = new VolumeService(authorizer);
    TableService tableService = new TableService(authorizer);
    FunctionService functionService = new FunctionService(authorizer);
    ModelService modelService = new ModelService(authorizer);
    MetastoreService metastoreService = new MetastoreService();
    // TODO: combine these into a single service in a follow-up PR
    TemporaryTableCredentialsService temporaryTableCredentialsService =
        new TemporaryTableCredentialsService(authorizer, credentialOperations);
    TemporaryVolumeCredentialsService temporaryVolumeCredentialsService =
        new TemporaryVolumeCredentialsService(authorizer, credentialOperations);
    TemporaryModelVersionCredentialsService temporaryModelVersionCredentialsService =
        new TemporaryModelVersionCredentialsService(authorizer, credentialOperations);
    TemporaryPathCredentialsService temporaryPathCredentialsService =
        new TemporaryPathCredentialsService(credentialOperations);
    sb.service("/", (ctx, req) -> HttpResponse.of("Hello, Unity Catalog!"))
        .annotatedService(controlPath + "auth", authService, unityConverterFunction)
        .annotatedService(
            controlPath + "scim2/Users",
            scim2UserService,
            unityConverterFunction,
            scimResponseFunction)
        .annotatedService(basePath + "permissions", permissionService)
        .annotatedService(basePath + "catalogs", catalogService, unityConverterFunction)
        .annotatedService(basePath + "schemas", schemaService, unityConverterFunction)
        .annotatedService(basePath + "volumes", volumeService, unityConverterFunction)
        .annotatedService(basePath + "tables", tableService, unityConverterFunction)
        .annotatedService(basePath + "functions", functionService, unityConverterFunction)
        .annotatedService(basePath + "models", modelService, unityConverterFunction)
        .annotatedService(basePath, metastoreService, unityConverterFunction)
        .annotatedService(
            basePath + "temporary-table-credentials", temporaryTableCredentialsService)
        .annotatedService(
            basePath + "temporary-volume-credentials", temporaryVolumeCredentialsService)
        .annotatedService(
            basePath + "temporary-model-version-credentials",
            temporaryModelVersionCredentialsService)
        .annotatedService(basePath + "temporary-path-credentials", temporaryPathCredentialsService);

    // Add support for Iceberg REST APIs
    ObjectMapper icebergMapper = RESTObjectMapper.mapper();
    JacksonRequestConverterFunction icebergRequestConverter =
        new JacksonRequestConverterFunction(icebergMapper);
    JacksonResponseConverterFunction icebergResponseConverter =
        new JacksonResponseConverterFunction(icebergMapper);
    MetadataService metadataService = new MetadataService(new FileIOFactory(credentialOperations));
    TableConfigService tableConfigService = new TableConfigService(credentialOperations);
    sb.annotatedService(
        basePath + "iceberg",
        new IcebergRestCatalogService(
            catalogService, schemaService, tableService, tableConfigService, metadataService),
        icebergRequestConverter,
        icebergResponseConverter);

    // TODO: eventually might want to make this secure-by-default.
    if (authorizationEnabled) {
      LOGGER.info("Authorization enabled.");

      // Note: Decorators are applied in reverse order.
      UnityAccessDecorator accessDecorator = new UnityAccessDecorator(authorizer);
      sb.routeDecorator().pathPrefix(basePath).build(accessDecorator);
      sb.routeDecorator()
          .pathPrefix(controlPath)
          .exclude(controlPath + "auth/tokens")
          .build(accessDecorator);

      AuthDecorator authDecorator = new AuthDecorator();
      sb.routeDecorator().pathPrefix(basePath).build(authDecorator);
      sb.routeDecorator()
          .pathPrefix(controlPath)
          .exclude(controlPath + "auth/tokens")
          .build(authDecorator);

      ExceptionHandlingDecorator exceptionDecorator =
          new ExceptionHandlingDecorator(new GlobalExceptionHandler());
      sb.decorator(exceptionDecorator);
    }
  }

  public static void main(String[] args) {
    OptionParser options = new OptionParser();
    options.parse(args);
    // Start Unity Catalog server
    UnityCatalogServer unityCatalogServer = new UnityCatalogServer(options.getPort() + 1);
    unityCatalogServer.printArt();
    unityCatalogServer.start();
    // Start URL transcoder
    Vertx vertx = Vertx.vertx();
    Verticle transcodeVerticle =
        new URLTranscoderVerticle(options.getPort(), options.getPort() + 1);
    vertx.deployVerticle(transcodeVerticle);
  }

  public void start() {
    LOGGER.info("Starting server...");
    MetastoreRepository.getInstance().initMetastoreIfNeeded();
    server.start().join();
  }

  public void stop() {
    server.stop().join();
    LOGGER.info("Server stopped.");
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
}
