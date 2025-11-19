package io.unitycatalog.server.base;

import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import lombok.SneakyThrows;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseServerTest {

  public static ServerConfig serverConfig = new ServerConfig("http://localhost", "");
  protected static UnityCatalogServer unityCatalogServer;
  protected static Properties serverProperties;
  protected static HibernateConfigurator hibernateConfigurator;
  protected static CloudCredentialVendor cloudCredentialVendor;

  // All test data should be written under this directory. It will be cleaned up.
  protected Path testDirectoryRoot;
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
    // Enable managed table creation for tests
    serverProperties.setProperty(Property.MANAGED_TABLE_ENABLED.getKey(), "true");
    tableStorageRoot = getManagedStorageCloudPath(testDirectoryRoot);
    serverProperties.setProperty(Property.TABLE_STORAGE_ROOT.getKey(), tableStorageRoot);
  }

  protected void setUpCredentialOperations() {}

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
      // start the server on a random port
      int port = TestUtils.getRandomPort();
      testDirectoryRoot =
          Paths.get(
                  System.getProperty("java.io.tmpdir"),
                  "BaseServerTest" + System.currentTimeMillis())
              .toAbsolutePath()
              .normalize();
      Files.createDirectories(testDirectoryRoot);

      setUpProperties();
      ServerProperties initServerProperties = new ServerProperties(serverProperties);
      setUpCredentialOperations();
      hibernateConfigurator = new HibernateConfigurator(initServerProperties);
      unityCatalogServer =
          UnityCatalogServer.builder()
              .port(port)
              .serverProperties(initServerProperties)
              .credentialOperations(cloudCredentialVendor)
              .build();
      unityCatalogServer.start();
      serverConfig.setServerUrl("http://localhost:" + port);
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
      tx.commit();
      session.close();

      unityCatalogServer.stop();
      if (testDirectoryRoot != null && Files.isDirectory(testDirectoryRoot)) {
        // Just make sure it doesn't wipe out root dir
        if (testDirectoryRoot.getNameCount() < 2) {
          throw new RuntimeException(
              "Test directory root path is too short: "
                  + testDirectoryRoot
                  + ". Refusing to delete.");
        }
        try {
          FileOperations.deleteLocalDirectory(testDirectoryRoot);
        } catch (IOException e) {
          // Ignore
        }
        testDirectoryRoot = null;
      }
    }
  }
}
