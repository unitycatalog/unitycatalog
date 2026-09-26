package io.unitycatalog.server.base;

import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import lombok.SneakyThrows;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public abstract class BaseServerTest {

  public static final ServerConfig serverConfig = new ServerConfig("http://localhost", "");

  /**
   * Base URL of the server's dedicated observability port (serves {@code /livez}, {@code /readyz},
   * {@code /metrics}). A second port on the same server as the API; set per test in {@link #setUp}.
   */
  protected static String observabilityUrl;

  protected UnityCatalogServer unityCatalogServer;
  protected Properties serverProperties;
  protected HibernateConfigurator hibernateConfigurator;
  protected CloudCredentialVendor cloudCredentialVendor;

  // All test data should be written under this directory. It will be cleaned up.
  @TempDir protected Path testDirectoryRoot;
  // The storage root URL for managed tables to be set in server properties.
  protected String tableStorageRoot;

  /**
   * This function should be overriden if the test wants to start UC server to take emulated cloud
   * path as managed storage. The emulated cloud FS is provided by subclasses of
   * CredentialTestFileSystem.
   */
  protected String managedStorageCloudScheme() {
    // By default, just use local FS for managed storage.
    return "file";
  }

  /** Returns string of the emulated cloud URL (or just the absolute local path) for a local path */
  protected String getManagedStorageCloudPath(Path localPath) {
    String localPathString;
    localPathString = localPath.toAbsolutePath().normalize().toString();
    String scheme = managedStorageCloudScheme();
    if (scheme.equals("file")) {
      return "file://" + localPathString;
    } else {
      return scheme + "://test-bucket0" + localPathString;
    }
  }

  protected void setUpProperties() {
    serverProperties = new Properties();
    serverProperties.setProperty(Property.SERVER_ENV.getKey(), "test");
    serverProperties.setProperty(Property.INCLUDE_STACK_TRACE_IN_ERROR.getKey(), "true");
    tableStorageRoot = getManagedStorageCloudPath(testDirectoryRoot);
    serverProperties.setProperty(Property.TABLE_STORAGE_ROOT.getKey(), tableStorageRoot);
  }

  protected void setUpCredentialOperations(ServerProperties serverProperties) {}

  /**
   * Subclasses can override this to customize the hibernate properties before the session factory
   * is created, e.g. to point the server at an external database such as PostgreSQL via
   * Testcontainers. Defaults to the H2 in-memory test configuration.
   */
  protected void setUpHibernateProperties(Properties hibernateProperties) {}

  @SneakyThrows
  @BeforeEach
  public void setUp() {
    if (serverConfig == null) {
      throw new IllegalArgumentException("Server config is required");
    }
    if (serverConfig.getServerUrl() == null) {
      throw new IllegalArgumentException("Server URL is required");
    }
    if (serverConfig.getAuthToken() == null) {
      throw new IllegalArgumentException("Auth token is required");
    }
    if (serverConfig.getServerUrl().contains("localhost")) {
      System.out.println("Running tests on localhost..");
      // start the server on a random port, with the observability endpoints on a second random
      // port (a distinct port on the same server, so parallel tests do not collide on it).
      int port = findAvailablePort();
      int observabilityPort = findAvailablePort();
      Files.createDirectories(testDirectoryRoot);

      setUpProperties();
      serverProperties.setProperty(
          Property.OBSERVABILITY_PORT.getKey(), String.valueOf(observabilityPort));
      ServerProperties initServerProperties = new ServerProperties(serverProperties);
      setUpCredentialOperations(initServerProperties);
      Properties hibernateProperties =
          HibernateConfigurator.setupHibernateProperties(initServerProperties);
      setUpHibernateProperties(hibernateProperties);
      hibernateConfigurator = new HibernateConfigurator(hibernateProperties);
      unityCatalogServer =
          UnityCatalogServer.builder()
              .port(port)
              .serverProperties(initServerProperties)
              .hibernateConfigurator(hibernateConfigurator)
              .credentialOperations(cloudCredentialVendor)
              .build();
      unityCatalogServer.start();
      serverConfig.setServerUrl("http://localhost:" + port);
      observabilityUrl = "http://localhost:" + observabilityPort;
    }
  }

  /** Issues a GET against the running test server (API port) and returns the string response. */
  @SneakyThrows
  protected static HttpResponse<String> httpGet(String path) {
    return httpGet(serverConfig.getServerUrl(), path);
  }

  /**
   * Issues a GET against the running test server's observability port ({@code /livez}, {@code
   * /readyz}, {@code /metrics}), which is a distinct port from the API listener.
   */
  @SneakyThrows
  protected static HttpResponse<String> httpGetObservability(String path) {
    return httpGet(observabilityUrl, path);
  }

  @SneakyThrows
  private static HttpResponse<String> httpGet(String baseUrl, String path) {
    HttpRequest request = HttpRequest.newBuilder().uri(URI.create(baseUrl + path)).GET().build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }

  /** Finds an available port for the UC server. */
  private int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @AfterEach
  public void tearDown() {
    if (unityCatalogServer != null) {

      // TODO: Figure out a better way to clear the database
      SessionFactory sessionFactory = hibernateConfigurator.getSessionFactory();
      Session session = sessionFactory.openSession();
      Transaction tx = session.beginTransaction();
      session.createMutationQuery("delete from FunctionParameterInfoDAO").executeUpdate();
      session.createMutationQuery("delete from FunctionInfoDAO").executeUpdate();
      session.createMutationQuery("delete from VolumeInfoDAO").executeUpdate();
      session.createMutationQuery("delete from ColumnInfoDAO").executeUpdate();
      session.createMutationQuery("delete from TableInfoDAO").executeUpdate();
      session.createMutationQuery("delete from StagingTableDAO").executeUpdate();
      session.createMutationQuery("delete from SchemaInfoDAO").executeUpdate();
      session.createMutationQuery("delete from CatalogInfoDAO").executeUpdate();
      session.createMutationQuery("delete from UserDAO").executeUpdate();
      session.createMutationQuery("delete from ExternalLocationDAO").executeUpdate();
      session.createMutationQuery("delete from CredentialDAO").executeUpdate();
      tx.commit();
      session.close();

      // close() rather than stop() so a server that built its own SessionFactory releases it;
      // this harness injects one, so the server leaves it open and we close it below.
      unityCatalogServer.close();
      // Release the factory and pool this harness built and injected in setUp(). setUp() builds
      // a fresh one per test, so leaked factories would otherwise accumulate for the whole JVM run.
      // In test env hbm2ddl is create-drop, so closing also drops the schema — keep this after the
      // cleanup queries above.
      hibernateConfigurator.close();
      // Null out so tearDown is idempotent if a subclass @AfterEach also invokes it.
      unityCatalogServer = null;
    }
  }
}
