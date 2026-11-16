package io.unitycatalog.client.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * The generated clients must decode response bodies as UTF-8, which the server always sends, and
 * not with the JVM default charset. The calls run in a child JVM whose default charset is
 * ISO-8859-1, so the test fails without the fix even where the default charset is UTF-8.
 */
public class ResponseCharsetTest {

  private static final String COMMENT = "café ☕ 😀";

  @Test
  public void testResponsesAreDecodedAsUtf8() throws Exception {
    Process process =
        new ProcessBuilder(
                Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dfile.encoding=ISO-8859-1",
                "-cp",
                System.getProperty("java.class.path"),
                Probe.class.getName())
            .redirectErrorStream(true)
            .start();
    List<String> output =
        new String(process.getInputStream().readAllBytes(), StandardCharsets.US_ASCII)
            .lines()
            .collect(Collectors.toList());
    assertThat(process.waitFor(60, TimeUnit.SECONDS)).isTrue();

    assertThat(output)
        .containsExactly(
            "charset: ISO-8859-1",
            "comment: " + escape(COMMENT),
            "error: " + escape(errorBody()),
            "exit");
  }

  private static String errorBody() {
    return "{\"error_code\":\"NOT_FOUND\",\"message\":\"Catalog not found: " + COMMENT + "\"}";
  }

  private static String escape(String s) {
    return s.chars().mapToObj(c -> String.format("\\u%04x", c)).collect(Collectors.joining());
  }

  /** Runs in the child JVM and prints its results as ASCII, so the console charset is moot. */
  public static class Probe {
    public static void main(String[] args) throws IOException {
      HttpServer server =
          HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      server.createContext(
          "/api/2.1/unity-catalog/catalogs/",
          exchange -> {
            boolean found = exchange.getRequestURI().getPath().endsWith("/c");
            byte[] body =
                (found ? "{\"name\":\"c\",\"comment\":\"" + COMMENT + "\"}" : errorBody())
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(found ? 200 : 404, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
              out.write(body);
            }
          });
      server.start();
      try {
        ApiClient client = new ApiClient();
        client.updateBaseUri(
            "http://localhost:" + server.getAddress().getPort() + "/api/2.1/unity-catalog");
        CatalogsApi catalogs = new CatalogsApi(client);
        System.out.println("charset: " + Charset.defaultCharset());
        System.out.println("comment: " + escape(catalogs.getCatalog("c", null).getComment()));
        try {
          catalogs.getCatalog("missing", null);
        } catch (ApiException e) {
          System.out.println("error: " + escape(e.getResponseBody()));
        }
        System.out.println("exit");
      } catch (ApiException e) {
        System.out.println("failed: " + e);
      } finally {
        server.stop(0);
      }
    }
  }
}
