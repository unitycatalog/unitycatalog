package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.server.Server;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A component on its own loopback listener, reached through UC's one public API port: what the
 * Vert.x transcoder in front of Armeria would otherwise be kept for.
 */
public class ArmeriaForwardTest {

  private static final String PREFIX = "/api/2.1/opensharing/";
  private static final String UNREACHABLE_PREFIX = "/api/2.1/unreachable/";

  private static BlockingTaskExecutor blockingTaskExecutor;
  private static Server component;
  private static Server uc;
  private static WebClient client;

  @BeforeAll
  public static void setUp() throws Exception {
    blockingTaskExecutor = BlockingTaskExecutor.builder().numThreads(1).build();
    // Echoes what it received, so the test can see exactly what the forward delivered.
    component =
        Server.builder()
            .localPort(0, SessionProtocol.HTTP)
            .gracefulShutdownTimeoutMillis(0, 0)
            .serviceUnder(
                PREFIX,
                (ctx, req) ->
                    HttpResponse.of(
                        req.aggregate()
                            .thenApply(
                                agg ->
                                    HttpResponse.builder()
                                        .status(
                                            agg.path().endsWith("/missing")
                                                ? HttpStatus.NOT_FOUND
                                                : HttpStatus.OK)
                                        .header("x-component", "yes")
                                        .content(
                                            MediaType.PLAIN_TEXT_UTF_8,
                                            String.join(
                                                "|",
                                                agg.method().name(),
                                                agg.path(),
                                                agg.authority(),
                                                agg.headers().get("authorization", "").toString(),
                                                agg.contentUtf8()))
                                        .build())))
            .build();
    component.start().join();
    URI target = URI.create("http://127.0.0.1:" + component.activeLocalPort());

    int port;
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0);
        ServerSocket closed = new ServerSocket(0)) {
      port = socket.getLocalPort();
      closedPort = closed.getLocalPort();
    }
    uc =
        new ArmeriaServerBuilder(
                port,
                "/api/2.1/unity-catalog/",
                "/api/1.0/unity-control/",
                new ServerProperties(new Properties()),
                blockingTaskExecutor)
            .forward(PREFIX, target)
            .forward(UNREACHABLE_PREFIX, URI.create("http://127.0.0.1:" + closedPort))
            .build();
    uc.start().join();
    client = WebClient.of("http://127.0.0.1:" + port);
  }

  @AfterAll
  public static void tearDown() {
    uc.stop().join();
    component.stop().join();
    blockingTaskExecutor.shutdown();
  }

  @Test
  public void forwardsPrefixedRequestsToTheComponent() {
    AggregatedHttpResponse response =
        client
            .execute(
                HttpRequest.of(
                    RequestHeaders.builder(HttpMethod.POST, PREFIX + "shares?maxResults=5")
                        .add("authorization", "Bearer recipient-token")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                    HttpData.ofUtf8("{\"name\":\"s1\"}")))
            .aggregate()
            .join();

    assertThat(response.status()).isEqualTo(HttpStatus.OK);
    assertThat(response.headers().get("x-component")).isEqualTo("yes");
    assertThat(response.contentUtf8())
        .isEqualTo(
            "POST|"
                + PREFIX
                + "shares?maxResults=5|127.0.0.1:"
                + component.activeLocalPort()
                + "|Bearer recipient-token|{\"name\":\"s1\"}");
  }

  @Test
  public void passesTheComponentsStatusThrough() {
    assertThat(client.get(PREFIX + "missing").aggregate().join().status())
        .isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  public void keepsServingUnityCatalogRoutes() {
    AggregatedHttpResponse root = client.get("/").aggregate().join();
    assertThat(root.contentUtf8()).isEqualTo("Hello, Unity Catalog!");
    assertThat(root.headers().get("x-component")).isNull();
    // Outside the prefix nothing is forwarded, even for a path the component would serve.
    assertThat(client.get("/api/2.1/opensharingX").aggregate().join().status())
        .isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  public void reportsAnUnreachableComponentAsAServerError() {
    assertThat(
            client.get(UNREACHABLE_PREFIX + "shares").aggregate().join().status().isServerError())
        .isTrue();
  }
}
