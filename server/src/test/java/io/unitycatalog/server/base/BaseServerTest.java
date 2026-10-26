package io.unitycatalog.server.base;

import com.linecorp.armeria.server.ServerPortBindException;
import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public abstract class BaseServerTest {

  public static final ServerConfig serverConfig = new ServerConfig("http://localhost", "");

  private static final int MAX_START_ATTEMPTS = 3;

  protected ServerConfig observabilityServerConfig;
  protected boolean observabilityEnabled;
  protected UnityCatalogServer unityCatalogServer;
  protected Properties serverProperties;
  protected HibernateConfigurator hibernateConfigurator;

  /** This test's database on the backend selected by UC_TEST_DATABASE. */
  protected TestDatabase testDatabase;

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
    serverProperties.setProperty(
        Property.EXTERNAL_LOCAL_ROOTS.getKey(), testDirectoryRoot.toString());
  }

  protected void setUpCredentialOperations(ServerProperties serverProperties) {}

  /**
   * Subclasses can override this to decorate the server's {@link FileOperations}, e.g. to map cloud
   * storage to local files for Iceberg tests. Returns the instance unchanged by default.
   */
  protected FileOperations decorateFileOperations(FileOperations fileOperations) {
    return fileOperations;
  }

  /**
   * Subclasses can override this to customize the hibernate properties before the session factory
   * is created. They start as this test's database from {@link TestDatabases}; a subclass that
   * points the server at its own database also sets {@code hibernate.hbm2ddl.auto} so each test
   * gets a fresh schema.
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
      Files.createDirectories(testDirectoryRoot);

      setUpProperties();
      ServerProperties initServerProperties = new ServerProperties(serverProperties);
      setUpCredentialOperations(initServerProperties);
      testDatabase = TestDatabases.newDatabase();
      Properties hibernateProperties = testDatabase.hibernateProperties(initServerProperties);
      setUpHibernateProperties(hibernateProperties);
      hibernateConfigurator = testDatabase.openHibernateConfigurator(hibernateProperties);
      startServerOnFreePorts(initServerProperties);
    }
  }

  /**
   * Starts the server on a random API port, plus a second random port for optional observability.
   * The ports are only free when chosen, so another process can bind one before the server does;
   * that start is retried with fresh ports.
   */
  private void startServerOnFreePorts(ServerProperties initServerProperties) throws IOException {
    for (int attempt = 1; ; attempt++) {
      int[] ports = findTwoAvailablePorts();
      int port = ports[0];
      int observabilityPort = ports[1];
      UnityCatalogServer.Builder serverBuilder =
          UnityCatalogServer.builder()
              .port(port)
              .serverProperties(initServerProperties)
              .hibernateConfigurator(hibernateConfigurator)
              .credentialOperations(cloudCredentialVendor)
              .fileOperationsDecorator(this::decorateFileOperations);
      if (observabilityEnabled) {
        serverBuilder.observabilityPort(observabilityPort);
      }
      unityCatalogServer = serverBuilder.build();
      try {
        unityCatalogServer.start();
      } catch (CompletionException e) {
        unityCatalogServer.close();
        unityCatalogServer = null;
        if (!(e.getCause() instanceof ServerPortBindException) || attempt == MAX_START_ATTEMPTS) {
          throw e;
        }
        continue;
      }
      serverConfig.setServerUrl("http://localhost:" + port);
      observabilityServerConfig = new ServerConfig("http://localhost:" + observabilityPort, "");
      return;
    }
  }

  /**
   * Returns two distinct free ports (for the API and observability listeners). Both sockets are
   * held open at once so the OS cannot return the same port twice, which a pair of sequential
   * {@code ServerSocket(0)} calls can, since each closes before the next opens.
   */
  private static int[] findTwoAvailablePorts() throws IOException {
    try (ServerSocket first = new ServerSocket(0);
        ServerSocket second = new ServerSocket(0)) {
      return new int[] {first.getLocalPort(), second.getLocalPort()};
    }
  }

  @AfterEach
  public void tearDown() {
    // No cleanup queries: each test's database is dropped below, or, for a subclass with its own
    // database, its create-drop schema is dropped when the configurator closes.
    try {
      if (unityCatalogServer != null) {
        // close() rather than stop() so a server that built its own SessionFactory releases it;
        // this harness injects one, so the server leaves it open; testDatabase.close() closes it.
        unityCatalogServer.close();
        // Null out so tearDown is idempotent if a subclass @AfterEach also invokes it.
        unityCatalogServer = null;
      }
    } finally {
      // Runs even if setUp() failed. Closes the configurator and removes the database, so it must
      // follow unityCatalogServer.close().
      if (testDatabase != null) {
        testDatabase.close();
        testDatabase = null;
      }
    }
  }
}
