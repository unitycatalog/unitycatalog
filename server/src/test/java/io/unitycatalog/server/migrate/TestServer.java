package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * The current server, in this process, on a {@link TestDatabase}: built like {@code
 * BaseServerTest.setUp()} (the database's own configurator, authorization on).
 */
final class TestServer implements AutoCloseable {

  private final UnityCatalogServer server;
  private final int port;
  private final String serviceToken;

  private TestServer(UnityCatalogServer server, int port, String serviceToken) {
    this.server = server;
    this.port = port;
    this.serviceToken = serviceToken;
  }

  /** Starts the server on {@code db}, with its table storage under {@code storageRoot}. */
  static TestServer start(TestDatabase db, Path storageRoot) {
    return start(db.openHibernateConfigurator(db.hibernateProperties()), storageRoot);
  }

  private static TestServer start(HibernateConfigurator configurator, Path storageRoot) {
    try {
      int port;
      try (ServerSocket socket = new ServerSocket(0)) {
        port = socket.getLocalPort();
      }
      Path configurationFolder = Files.createDirectories(storageRoot.resolve("conf"));
      Properties properties = new Properties();
      properties.setProperty(Property.SERVER_ENV.getKey(), "test");
      properties.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");
      properties.setProperty(Property.TABLE_STORAGE_ROOT.getKey(), storageRoot.toUri().toString());
      properties.setProperty(Property.EXTERNAL_LOCAL_ROOTS.getKey(), storageRoot.toString());
      UnityCatalogServer server =
          UnityCatalogServer.builder()
              .port(port)
              .serverProperties(new ServerProperties(properties))
              .hibernateConfigurator(configurator)
              .configurationFolder(configurationFolder)
              .build();
      try {
        server.start();
      } catch (Throwable t) {
        server.close();
        throw t;
      }
      // The server keeps no token file, so mint one from the signing keys it generated.
      SecurityContext securityContext =
          new SecurityContext(
              configurationFolder,
              new SecurityConfiguration(configurationFolder),
              "server",
              INTERNAL);
      return new TestServer(server, port, securityContext.createServiceToken());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** A client for this server, authenticated with the server's own service token. */
  ApiClient apiClient() {
    return createApiClient(new ServerConfig("http://localhost:" + port, serviceToken));
  }

  @Override
  public void close() {
    server.close();
  }
}
