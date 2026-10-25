package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.utils.TestUtils.createApiClient;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.base.ServerConfig;
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
 * BaseServerTest.setUp()} (the database's own configurator, authorization on). {@link #start}
 * throws what the server throws when it refuses the database's schema.
 */
final class TestServer implements AutoCloseable {

  private final UnityCatalogServer server;
  private final int port;

  private TestServer(UnityCatalogServer server, int port) {
    this.server = server;
    this.port = port;
  }

  /** Starts the server on {@code db}, with its table storage under {@code storageRoot}. */
  static TestServer start(TestDatabase db, Path storageRoot) {
    return start(db, db.hibernateProperties(), storageRoot);
  }

  /** The same, with the given Hibernate properties for {@code db}. */
  static TestServer start(TestDatabase db, Properties hibernateProperties, Path storageRoot) {
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
              .hibernateConfigurator(db.openHibernateConfigurator(hibernateProperties))
              .build();
      try {
        server.start();
      } catch (Throwable t) {
        server.close();
        throw t;
      }
      return new TestServer(server, port);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** A client for this server, authenticated with the server's own service token. */
  ApiClient apiClient() {
    try {
      String token = Files.readString(Path.of("etc/conf/token.txt")).trim();
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
