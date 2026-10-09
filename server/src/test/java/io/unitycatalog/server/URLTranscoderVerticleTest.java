package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
        .deployVerticle(new URLTranscoderVerticle(transcodePort, servicePort, 1))
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
  public void testConfiguredPoolAllowsMoreThanTheVertxDefault() throws Exception {
    // Vert.x WebClient defaults to 5. A configured size of 6 must all be in flight together.
    assertConcurrentRequests(/* backendPoolSize= */ 6, /* sent= */ 6, /* inFlight= */ 6);
  }

  @Test
  public void testConfiguredPoolSizeIsTheInFlightCap() throws Exception {
    // One more request than the configured pool must wait. That is what keeps the proxy aligned
    // with the capacity the caller passed in.
    assertConcurrentRequests(/* backendPoolSize= */ 2, /* sent= */ 3, /* inFlight= */ 2);
  }

  private static void assertConcurrentRequests(int backendPoolSize, int sent, int inFlight)
      throws Exception {
    CountDownLatch entered = new CountDownLatch(inFlight);
    CountDownLatch overflow = new CountDownLatch(inFlight + 1);
    List<HttpServerResponse> parked = Collections.synchronizedList(new ArrayList<>());
    AtomicBoolean release = new AtomicBoolean(false);
    int backendPort = findAvailablePort();
    int frontPort = findAvailablePort();
    vertx
        .createHttpServer()
        .requestHandler(
            request -> {
              synchronized (parked) {
                // Arrivals after the cap check complete immediately, including the request that
                // was waiting for a pool slot.
                if (release.get()) {
                  request.response().end("ok");
                  return;
                }
                parked.add(request.response());
              }
              entered.countDown();
              overflow.countDown();
            })
        .listen(backendPort)
        .toCompletionStage()
        .toCompletableFuture()
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    vertx
        .deployVerticle(new URLTranscoderVerticle(frontPort, backendPort, backendPoolSize))
        .toCompletionStage()
        .toCompletableFuture()
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    WebClient caller = WebClient.create(vertx, new WebClientOptions().setMaxPoolSize(sent));
    List<CompletableFuture<HttpResponse<Buffer>>> calls = new ArrayList<>();
    try {
      for (int i = 0; i < sent; i++) {
        calls.add(
            caller
                .request(HttpMethod.GET, frontPort, HOST, "/hold")
                .send()
                .toCompletionStage()
                .toCompletableFuture());
      }
      assertThat(entered.await(2, TimeUnit.SECONDS))
          .as("requests in flight at the backend")
          .isTrue();
      if (sent > inFlight) {
        assertThat(overflow.await(1, TimeUnit.SECONDS))
            .as("a request beyond the configured pool must wait")
            .isFalse();
      }
      end(parked, release);
      for (CompletableFuture<HttpResponse<Buffer>> call : calls) {
        assertThat(call.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
      }
    } finally {
      end(parked, release);
      caller.close();
    }
  }

  /** Completes any backend response still held open. Later arrivals finish themselves. */
  private static void end(List<HttpServerResponse> parked, AtomicBoolean release)
      throws InterruptedException {
    CountDownLatch done = new CountDownLatch(1);
    vertx.runOnContext(
        ignored -> {
          List<HttpServerResponse> held;
          synchronized (parked) {
            release.set(true);
            held = List.copyOf(parked);
          }
          for (HttpServerResponse response : held) {
            if (!response.ended()) {
              response.end("ok");
            }
          }
          done.countDown();
        });
    assertThat(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void testDeploymentFailsWhenTheTranscodePortIsTaken() throws Exception {
    try (ServerSocket occupied = new ServerSocket(0)) {
      CompletableFuture<String> deployment =
          vertx
              .deployVerticle(new URLTranscoderVerticle(occupied.getLocalPort(), servicePort, 1))
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
