package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class URLTranscoderVerticleTest {

  private static final String HOST = "127.0.0.1";
  private static final int TIMEOUT_SECONDS = 10;

  private static Vertx vertx;
  private static WebClient client;
  private static int transcodePort;
  private static int servicePort;

  @BeforeAll
  public static void setUp() throws Exception {
    vertx = Vertx.vertx();
    client = WebClient.create(vertx);
    servicePort = findAvailablePort();
    transcodePort = findAvailablePort();
    startService();
    vertx
        .deployVerticle(new URLTranscoderVerticle(transcodePort, servicePort))
        .toCompletionStage()
        .toCompletableFuture()
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (vertx != null) {
      vertx
          .close()
          .toCompletionStage()
          .toCompletableFuture()
          .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testNoContentResponseIsTranscoded() throws Exception {
    HttpResponse<Buffer> response = send(HttpMethod.GET, "/no-content");

    assertThat(response.statusCode()).isEqualTo(204);
    assertThat(response.body()).isNull();
  }

  @Test
  public void testHeadResponseIsTranscoded() throws Exception {
    HttpResponse<Buffer> response = send(HttpMethod.HEAD, "/empty-body");

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isNull();
  }

  @Test
  public void testResponseWithBodyIsTranscoded() throws Exception {
    HttpResponse<Buffer> response = send(HttpMethod.GET, "/with-body");

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.bodyAsString()).isEqualTo("transcoded");
  }

  @Test
  public void testUnitSeparatorInPathIsDecoded() throws Exception {
    HttpResponse<Buffer> response = send(HttpMethod.GET, "/echo/catalog%1Fschema");

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.bodyAsString()).isEqualTo("/echo/catalog.schema");
  }

  private static HttpResponse<Buffer> send(HttpMethod method, String path) throws Exception {
    // A bounded wait, so a response that is never written fails the test instead of hanging it.
    return client
        .request(method, transcodePort, HOST, path)
        .send()
        .toCompletionStage()
        .toCompletableFuture()
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  public void testDeploymentFailsWhenTheTranscodePortIsTaken() throws Exception {
    try (ServerSocket occupied = new ServerSocket(0)) {
      CompletableFuture<String> deployment =
          vertx
              .deployVerticle(new URLTranscoderVerticle(occupied.getLocalPort(), servicePort))
              .toCompletionStage()
              .toCompletableFuture();

      // A transcoder that cannot bind leaves clients with no port to talk to, so its deployment
      // has to fail rather than report success and log the reason.
      assertThatThrownBy(() -> deployment.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
          .hasRootCauseInstanceOf(BindException.class);
    }
  }

  private static void startService() throws Exception {
    vertx
        .createHttpServer()
        .requestHandler(
            request -> {
              switch (request.path()) {
                case "/no-content":
                  request.response().setStatusCode(204).end();
                  break;
                case "/with-body":
                  request.response().end("transcoded");
                  break;
                case "/empty-body":
                  request.response().end();
                  break;
                default:
                  request.response().end(request.path());
              }
            })
        .listen(servicePort)
        .toCompletionStage()
        .toCompletableFuture()
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private static int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
