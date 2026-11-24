package io.unitycatalog.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.unitycatalog.cli.delta.DeltaKernelUtils;
import io.unitycatalog.client.auth.TokenProvider;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Checks which URL each CLI client sends its requests to for a given {@code --server}. */
public class UnityCatalogCliTest {

  private HttpServer server;
  private String responseBody;
  private final List<String> requestPaths = new CopyOnWriteArrayList<>();
  private final List<String> authorizationHeaders = new CopyOnWriteArrayList<>();

  @BeforeEach
  public void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          requestPaths.add(exchange.getRequestURI().getRawPath());
          authorizationHeaders.add(
              String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
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
        Arguments.of("/my%20gateway", "/my%20gateway"),
        Arguments.of("/api/2.1/unity-catalog", ""),
        Arguments.of("/api/2.1/unity-catalog/", ""),
        Arguments.of("/api/1.0/unity-control", ""),
        Arguments.of("/api/1.0/unity-control/", ""),
        Arguments.of("/uc/api/2.1/unity-catalog", "/uc"),
        Arguments.of("/uc//api/1.0/unity-control/", "/uc"),
        Arguments.of("/api/2.1/unity-catalog/uc", "/api/2.1/unity-catalog/uc"),
        Arguments.of("/api/2.1", "/api/2.1"));
  }

  @ParameterizedTest
  @MethodSource("serverPaths")
  public void testCatalogClientKeepsServerPath(String serverPath, String expectedPrefix) {
    responseBody = "{\"catalogs\":[]}";
    runCli(serverPath, "catalog", "list");
    assertThat(requestPaths).containsExactly(expectedPrefix + "/api/2.1/unity-catalog/catalogs");
    assertThat(authorizationHeaders).containsExactly("Bearer test-token");
  }

  @ParameterizedTest
  @MethodSource("serverPaths")
  public void testControlClientKeepsServerPath(String serverPath, String expectedPrefix) {
    responseBody = "{\"Resources\":[]}";
    runCli(serverPath, "user", "list");
    assertThat(requestPaths).containsExactly(expectedPrefix + "/api/1.0/unity-control/scim2/Users");
    assertThat(authorizationHeaders).containsExactly("Bearer test-token");
  }

  @ParameterizedTest
  @MethodSource("serverPaths")
  public void testAuthLoginKeepsServerPath(String serverPath, String expectedPrefix) {
    responseBody = "{\"access_token\":\"exchanged-token\"}";
    JsonNode output = runCli(serverPath, "auth", "login", "--identity_token", "identity-token");
    assertThat(requestPaths).containsExactly(expectedPrefix + "/api/1.0/unity-control/auth/tokens");
    assertThat(output.get("access_token").asText()).isEqualTo("exchanged-token");
  }

  static Stream<Arguments> deltaServerPaths() {
    // Delta's client builds its base path from the decoded URI path, so an encoded path such as
    // /my%20gateway fails there whatever URI the CLI passes.
    return serverPaths().filter(arguments -> !((String) arguments.get()[0]).contains("%"));
  }

  @ParameterizedTest
  @MethodSource("deltaServerPaths")
  public void testManagedTableCreateHandsServerUriToDeltaCommitClient(
      String serverPath, String expectedPrefix) throws Exception {
    responseBody =
        "{\"id\":\"staging-id\",\"staging_location\":\"file:///tmp/staging\","
            + "\"metastore_id\":\"metastore-id\"}";
    List<String> commitClientUris = new CopyOnWriteArrayList<>();
    try (MockedStatic<DeltaKernelUtils> deltaKernelUtils =
        Mockito.mockStatic(DeltaKernelUtils.class)) {
      deltaKernelUtils
          .when(
              () ->
                  DeltaKernelUtils.createDeltaTable(
                      any(), any(), any(), any(), any(), any(), any()))
          .thenAnswer(
              invocation -> {
                commitClientUris.add(invocation.getArgument(4));
                return Map.of();
              });
      runCli(
          serverPath,
          "table",
          "create",
          "--full_name",
          "c.s.t",
          "--columns",
          "id INT",
          "--table_type",
          "MANAGED");
    }
    assertThat(commitClientUris).hasSize(1);

    requestPaths.clear();
    try (UCTokenBasedRestClient commitClient =
        new UCTokenBasedRestClient(
            commitClientUris.get(0),
            TokenProvider.create(Map.of("type", "static", "token", "test-token")),
            Map.of())) {
      assertThat(commitClient.getMetastoreId()).isEqualTo("metastore-id");
    }
    assertThat(requestPaths)
        .containsExactly(expectedPrefix + "/api/2.1/unity-catalog/metastore_summary");
  }

  private JsonNode runCli(String serverPath, String... args) {
    String serverUrl = "http://127.0.0.1:" + server.getAddress().getPort() + serverPath;
    return TestUtils.executeCliCommand(
        Stream.concat(
                Stream.of("--server", serverUrl, "--auth_token", "test-token", "--output", "json"),
                Stream.of(args))
            .toList());
  }
}
