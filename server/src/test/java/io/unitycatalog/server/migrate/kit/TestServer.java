package io.unitycatalog.server.migrate.kit;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.IOException;
import java.io.Reader;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * The current server, in this process, on a {@link TestDatabase}: built like {@code
 * BaseServerTest.setUp()} (the database's own configurator, authorization on). {@link #start}
 * throws what the server throws when it refuses the database's schema.
 */
public final class TestServer implements AutoCloseable {

  private final UnityCatalogServer server;
  private final Path storageRoot;
  private final int port;

  private TestServer(UnityCatalogServer server, Path storageRoot, int port) {
    this.server = server;
    this.storageRoot = storageRoot;
    this.port = port;
  }

  public static TestServer start(TestDatabase db) {
    return start(db, List.of());
  }

  /** Also applies the schema steps in {@code extraMigrationLocations} (tests' own steps). */
  public static TestServer start(TestDatabase db, List<String> extraMigrationLocations) {
    return start(db.openHibernateConfigurator(db.hibernateProperties()), extraMigrationLocations);
  }

  /**
   * Starts the server in a process of its own, for a test that stops it with a signal: {@code args}
   * are a file of the database's Hibernate properties, then the extra migration locations.
   */
  public static void main(String[] args) throws IOException {
    Properties hibernateProperties = new Properties();
    try (Reader in = Files.newBufferedReader(Path.of(args[0]))) {
      hibernateProperties.load(in);
    }
    start(new HibernateConfigurator(hibernateProperties), List.of(args).subList(1, args.length));
  }

  private static TestServer start(
      HibernateConfigurator hibernateConfigurator, List<String> extraMigrationLocations) {
    try {
      Path storageRoot = Files.createTempDirectory("uc-test-server");
      try {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
          port = socket.getLocalPort();
        }
        Properties properties = new Properties();
        properties.setProperty(Property.SERVER_ENV.getKey(), "test");
        properties.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");
        properties.setProperty(
            Property.TABLE_STORAGE_ROOT.getKey(), storageRoot.toUri().toString());
        properties.setProperty(Property.EXTERNAL_LOCAL_ROOTS.getKey(), storageRoot.toString());
        UnityCatalogServer server =
            UnityCatalogServer.builder()
                .port(port)
                .serverProperties(new ServerProperties(properties))
                .hibernateConfigurator(hibernateConfigurator)
                .extraMigrationLocations(extraMigrationLocations)
                .build();
        try {
          server.start();
        } catch (Throwable t) {
          server.close();
          throw t;
        }
        return new TestServer(server, storageRoot, port);
      } catch (Throwable t) {
        delete(storageRoot);
        throw t;
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** A client for this server, authenticated with the server's own service token. */
  public ApiClient apiClient() {
    try {
      String token = Files.readString(Path.of("etc/conf/token.txt")).trim();
      return ApiClientBuilder.create()
          .uri(URI.create("http://localhost:" + port))
          .tokenProvider(TokenProvider.create(Map.of("type", "static", "token", token)))
          .retryPolicy(JitterDelayRetryPolicy.builder().maxAttempts(1).build())
          .build();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void close() {
    try {
      server.close();
    } finally {
      delete(storageRoot);
    }
  }

  private static void delete(Path root) {
    try (Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    } catch (IOException e) {
      // A leftover temp directory is harmless.
    }
  }
}
