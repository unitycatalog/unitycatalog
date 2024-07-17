package io.unitycatalog.server.base;

import java.util.Collection;
import java.util.List;

import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseServerTest {

    public ServerConfig serverConfig;
    protected UnityCatalogServer unityCatalogServer;

    /**
     * To test against UC server, add
     * UC server endpoint as server URL and PAT token as auth token
     * e.g. ServerConfig("https://<server-url>", "<PAT-token>")
     * Multiple server configurations can be added to test against multiple servers
     * @return
     */
    public static Collection<ServerConfig> data() {
        return List.of(
                new ServerConfig("http://localhost", "")
        );
    }

    protected void cleanUp() {

    }

    @BeforeEach
    public void setUp() {
        serverConfig = data().iterator().next();
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
        cleanUp();
        if (unityCatalogServer != null) {
            unityCatalogServer.stop();
        }
    }
}
