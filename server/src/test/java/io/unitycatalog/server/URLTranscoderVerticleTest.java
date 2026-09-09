package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
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

  @BeforeAll
  public static void setUp() throws Exception {
    vertx = Vertx.vertx();
    client = WebClient.create(vertx);
    int servicePort = findAvailablePort();
    transcodePort = findAvailablePort();
    startService(servicePort);
    vertx
        .deployVerticle(new URLTranscoderVerticle(transcodePort, servicePort))
        .toCompletionStage()
        .toCompletableFuture()
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    awaitPort(transcodePort);
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

  @Test
  public void testNoSidecarRoutesEverythingToService() {
    URLTranscoderVerticle verticle = new URLTranscoderVerticle(0, 9000);

    assertThat(verticle.targetPort("/api/2.1/unity-catalog/catalogs")).isEqualTo(9000);
    assertThat(verticle.targetPort("/api/2.1/opensharing/provider/shares")).isEqualTo(9000);
  }

  @Test
  public void testPathMatchingASidecarPrefixRoutesThere() {
    URLTranscoderVerticle verticle =
        new URLTranscoderVerticle(
            0,
            9000,
            9099,
            List.of(
                "/api/2.1/opensharing",
                "/api/2.1/opensharing/provider",
                "/api/2.1/opensharing/activation"));

    assertThat(verticle.targetPort("/api/2.1/opensharing/shares")).isEqualTo(9099);
    assertThat(verticle.targetPort("/api/2.1/opensharing/provider/shares")).isEqualTo(9099);
    assertThat(verticle.targetPort("/api/2.1/opensharing/activation/some-nonce")).isEqualTo(9099);
  }

  @Test
  public void testPathMatchingNoSidecarPrefixRoutesToService() {
    URLTranscoderVerticle verticle =
        new URLTranscoderVerticle(0, 9000, 9099, List.of("/api/2.1/opensharing"));

    assertThat(verticle.targetPort("/api/2.1/unity-catalog/catalogs")).isEqualTo(9000);
  }

  @Test
  public void testSidecarPathIsForwardedToTheSidecarPort() throws Exception {
    int servicePort = findAvailablePort();
    int sidecarPort = findAvailablePort();
    int routedTranscodePort = findAvailablePort();
    startService(servicePort);
    startSidecar(sidecarPort);
    vertx
        .deployVerticle(
            new URLTranscoderVerticle(
                routedTranscodePort, servicePort, sidecarPort, List.of("/sidecar")))
        .toCompletionStage()
        .toCompletableFuture()
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    awaitPort(routedTranscodePort);

    HttpResponse<Buffer> viaSidecar =
        client
            .request(HttpMethod.GET, routedTranscodePort, HOST, "/sidecar/hello")
            .send()
            .toCompletionStage()
            .toCompletableFuture()
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(viaSidecar.bodyAsString()).isEqualTo("from-sidecar:/sidecar/hello");

    HttpResponse<Buffer> viaService =
        client
            .request(HttpMethod.GET, routedTranscodePort, HOST, "/with-body")
            .send()
            .toCompletionStage()
            .toCompletableFuture()
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(viaService.bodyAsString()).isEqualTo("transcoded");
  }

  private static void startSidecar(int sidecarPort) throws Exception {
    vertx
        .createHttpServer()
        .requestHandler(request -> request.response().end("from-sidecar:" + request.path()))
        .listen(sidecarPort)
        .toCompletionStage()
        .toCompletableFuture()
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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

  private static void startService(int servicePort) throws Exception {
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

  /** The verticle binds its port asynchronously, so wait for it before sending any request. */
  private static void awaitPort(int port) throws Exception {
    for (int attempt = 0; attempt < 100; attempt++) {
      try (Socket ignored = new Socket(HOST, port)) {
        return;
      } catch (IOException e) {
        Thread.sleep(50);
      }
    }
    throw new IllegalStateException("URL transcoder did not start on port " + port);
  }

  private static int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
