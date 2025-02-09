package io.unitycatalog.server.base;

import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Properties;
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
  protected static CredentialOperations credentialOperations;

  protected void setUpProperties() {
    serverProperties = new Properties();
    serverProperties.setProperty("server.env", "test");
  }

  protected void setUpCredentialOperations() {}

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
      setUpProperties();
      ServerProperties initServerProperties = new ServerProperties(serverProperties);
      setUpCredentialOperations();
      hibernateConfigurator = new HibernateConfigurator(initServerProperties);
      unityCatalogServer =
          UnityCatalogServer.builder()
              .port(port)
              .serverProperties(initServerProperties)
              .credentialOperations(credentialOperations)
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
      session.createMutationQuery("delete from SchemaInfoDAO").executeUpdate();
      session.createMutationQuery("delete from CatalogInfoDAO").executeUpdate();
      session.createMutationQuery("delete from UserDAO").executeUpdate();
      tx.commit();
      session.close();

      unityCatalogServer.stop();
    }
  }
}
