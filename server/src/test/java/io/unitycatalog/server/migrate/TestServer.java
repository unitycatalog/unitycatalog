package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.utils.TestUtils.createApiClient;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
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
  private final Path configurationFolder;

  private TestServer(UnityCatalogServer server, int port, Path configurationFolder) {
    this.server = server;
    this.port = port;
    this.configurationFolder = configurationFolder;
  }

  /**
   * Starts the server on {@code db}.
   *
   * @param db the database to run on
   * @param storageRoot the root of the server's table storage
   * @param configurationFolder the folder for the server's signing keys, {@code certs.json} and
   *     {@code token.txt}, instead of the shared {@code etc/conf}
   * @return the started server
   */
  static TestServer start(TestDatabase db, Path storageRoot, Path configurationFolder) {
    return start(
        db.openHibernateConfigurator(db.hibernateProperties()), storageRoot, configurationFolder);
  }

  private static TestServer start(
      HibernateConfigurator configurator, Path storageRoot, Path configurationFolder) {
    try {
      int port;
      try (ServerSocket socket = new ServerSocket(0)) {
        port = socket.getLocalPort();
      }
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
      return new TestServer(server, port, configurationFolder);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** A client for this server, authenticated with the server's own service token. */
  ApiClient apiClient() {
    try {
      String token = Files.readString(configurationFolder.resolve("token.txt")).trim();
      return createApiClient(new ServerConfig("http://localhost:" + port, token));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    server.close();
  }
}
