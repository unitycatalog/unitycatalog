package io.unitycatalog.server;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.annotation.JacksonRequestConverterFunction;
import com.linecorp.armeria.server.annotation.JacksonResponseConverterFunction;
import com.linecorp.armeria.server.docs.DocService;
import io.unitycatalog.server.service.CatalogService;
import io.unitycatalog.server.service.FunctionService;
import io.unitycatalog.server.service.IcebergRestCatalogService;
import io.unitycatalog.server.service.ModelService;
import io.unitycatalog.server.service.SchemaService;
import io.unitycatalog.server.service.TableService;
import io.unitycatalog.server.service.TemporaryTableCredentialsService;
import io.unitycatalog.server.service.TemporaryVolumeCredentialsService;
import io.unitycatalog.server.service.VolumeService;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.service.iceberg.FileIOFactory;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.utils.RESTObjectMapper;
import io.unitycatalog.server.utils.VersionUtils;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnityCatalogServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnityCatalogServer.class);

  static {
    System.setProperty("log4j.configurationFile", "etc/conf/server.log4j2.properties");
    Configurator.initialize(null, "etc/conf/server.log4j2.properties");
  }

  Server server;
  private static final String basePath = "/api/2.1/unity-catalog/";

  public UnityCatalogServer() {
    new UnityCatalogServer(8080);
  }

  public UnityCatalogServer(int port) {
    ServerBuilder sb = Server.builder().serviceUnder("/docs", new DocService()).http(port);
    addServices(sb);

    server = sb.build();
  }

  private void addServices(ServerBuilder sb) {
    ObjectMapper unityMapper =
        JsonMapper.builder().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    JacksonRequestConverterFunction unityConverterFunction =
        new JacksonRequestConverterFunction(unityMapper);

    // Credentials Service
    CredentialOperations credentialOperations = new CredentialOperations();

    // Add support for Unity Catalog APIs
    CatalogService catalogService = new CatalogService();
    SchemaService schemaService = new SchemaService();
    VolumeService volumeService = new VolumeService();
    TableService tableService = new TableService();
    FunctionService functionService = new FunctionService();
    ModelService modelService = new ModelService();
    TemporaryTableCredentialsService temporaryTableCredentialsService =
        new TemporaryTableCredentialsService(credentialOperations);
    TemporaryVolumeCredentialsService temporaryVolumeCredentialsService =
        new TemporaryVolumeCredentialsService();
    sb.service("/", (ctx, req) -> HttpResponse.of("Hello, Unity Catalog!"))
        .annotatedService(basePath + "catalogs", catalogService, unityConverterFunction)
        .annotatedService(basePath + "schemas", schemaService, unityConverterFunction)
        .annotatedService(basePath + "volumes", volumeService, unityConverterFunction)
        .annotatedService(basePath + "tables", tableService, unityConverterFunction)
        .annotatedService(basePath + "functions", functionService, unityConverterFunction)
        .annotatedService(basePath + "models", modelService, unityConverterFunction)
        .annotatedService(
            basePath + "temporary-table-credentials", temporaryTableCredentialsService)
        .annotatedService(
            basePath + "temporary-volume-credentials", temporaryVolumeCredentialsService);

    // Add support for Iceberg REST APIs
    ObjectMapper icebergMapper = RESTObjectMapper.mapper();
    JacksonRequestConverterFunction icebergRequestConverter =
        new JacksonRequestConverterFunction(icebergMapper);
    JacksonResponseConverterFunction icebergResponseConverter =
        new JacksonResponseConverterFunction(icebergMapper);
    MetadataService metadataService = new MetadataService(new FileIOFactory(credentialOperations));
    sb.annotatedService(
        basePath + "iceberg",
        new IcebergRestCatalogService(catalogService, schemaService, tableService, metadataService),
        icebergRequestConverter,
        icebergResponseConverter);
  }

  public static void main(String[] args) {
    int port = 8080;
    Options options = new Options();
    options.addOption(
        Option.builder("p")
            .longOpt("port")
            .hasArg()
            .desc("Port number to run the server on. Default is 8080.")
            .type(Integer.class)
            .build());
    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cmd = parser.parse(options, args);
      if (cmd.hasOption("p")) {
        port = cmd.getParsedOptionValue("p");
      }
    } catch (ParseException e) {
      System.out.println();
      System.out.println("Parsing Failed. Reason: " + e.getMessage());
      System.out.println();
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("bin/start-uc-server", options);
      return;
    }
    // Start Unity Catalog server
    UnityCatalogServer unityCatalogServer = new UnityCatalogServer(port + 1);
    unityCatalogServer.printArt();
    unityCatalogServer.start();
    // Start URL transcoder
    Vertx vertx = Vertx.vertx();
    Verticle transcodeVerticle = new URLTranscoderVerticle(port, port + 1);
    vertx.deployVerticle(transcodeVerticle);
  }

  public void start() {
    LOGGER.info("Starting server...");
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
