package io.unitycatalog.server.base;

import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseServerTest {

  public static ServerConfig serverConfig = new ServerConfig("http://localhost", "");
  private static UnityCatalogServer unityCatalogServer;

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
      System.setProperty("server.env", "test");
      unityCatalogServer = new UnityCatalogServer(port);
      unityCatalogServer.start();
      serverConfig.setServerUrl("http://localhost:" + port);
    }
  }

  @AfterEach
  public void tearDown() {
    if (unityCatalogServer != null) {
      unityCatalogServer.stop();
    }
  }
}
