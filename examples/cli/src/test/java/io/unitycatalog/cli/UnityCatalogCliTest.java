package io.unitycatalog.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Checks which URL each CLI client sends its requests to for a given {@code --server}. */
public class UnityCatalogCliTest {

  private HttpServer server;
  private String responseBody;
  private final List<String> requestPaths = new CopyOnWriteArrayList<>();

  @BeforeEach
  public void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          requestPaths.add(exchange.getRequestURI().getRawPath());
          byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
  }

  @AfterEach
  public void tearDown() {
    server.stop(0);
  }

  static Stream<Arguments> serverPaths() {
    return Stream.of(
        Arguments.of("", ""),
        Arguments.of("/", ""),
        Arguments.of("/uc", "/uc"),
        Arguments.of("/uc/", "/uc"),
        Arguments.of("/gateway/uc//", "/gateway/uc"),
        Arguments.of("/my%20gateway", "/my%20gateway"));
  }

  @ParameterizedTest
  @MethodSource("serverPaths")
  public void testCatalogClientKeepsServerPath(String serverPath, String expectedPrefix) {
    responseBody = "{\"catalogs\":[]}";
    runCli(serverPath, "catalog", "list");
    assertThat(requestPaths).containsExactly(expectedPrefix + "/api/2.1/unity-catalog/catalogs");
  }

  @ParameterizedTest
  @MethodSource("serverPaths")
  public void testControlClientKeepsServerPath(String serverPath, String expectedPrefix) {
    responseBody = "{\"Resources\":[]}";
    runCli(serverPath, "user", "list");
    assertThat(requestPaths).containsExactly(expectedPrefix + "/api/1.0/unity-control/scim2/Users");
  }

  @ParameterizedTest
  @MethodSource("serverPaths")
  public void testAuthLoginKeepsServerPath(String serverPath, String expectedPrefix) {
    responseBody = "{\"access_token\":\"exchanged-token\"}";
    runCli(serverPath, "auth", "login", "--identity_token", "identity-token");
    assertThat(requestPaths).containsExactly(expectedPrefix + "/api/1.0/unity-control/auth/tokens");
  }

  private void runCli(String serverPath, String... args) {
    String serverUrl = "http://127.0.0.1:" + server.getAddress().getPort() + serverPath;
    TestUtils.executeCliCommand(
        Stream.concat(Stream.of("--server", serverUrl, "--output", "json"), Stream.of(args))
            .toList());
  }
}
