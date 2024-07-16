package io.unitycatalog.server.base;

import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.utils.TestUtils;

import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class BaseServerTest {

  @Parameterized.Parameter public static ServerConfig serverConfig;
  private static UnityCatalogServer unityCatalogServer;

  /**
   * To test against UC server, add UC server endpoint as server URL and PAT token as auth token
   * e.g. ServerConfig("https://<server-url>", "<PAT-token>") Multiple server configurations can be
   * added to test against multiple servers
   *
   * @return
   */
  @Parameterized.Parameters
  public static Collection<ServerConfig> data() {
    return List.of(new ServerConfig("http://localhost", ""));
  }

  @Before
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
      System.out.println("Running tests on localhost");
      System.out.println("CWD: " + Paths.get(".").toAbsolutePath().normalize());
      // start the server on a random port
      int port = TestUtils.getRandomPort();
      System.setProperty("server.env", "test");
      unityCatalogServer = new UnityCatalogServer(port);
      unityCatalogServer.start();
      serverConfig.setServerUrl("http://localhost:" + port);
    }
  }

  @After
  public void tearDown() {
    if (unityCatalogServer != null) {
      unityCatalogServer.stop();
    }
  }
}
