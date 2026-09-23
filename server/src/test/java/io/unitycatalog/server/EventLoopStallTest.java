package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.annotation.Get;
import io.unitycatalog.server.service.UnityCatalogRestService;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * One HTTP/2 connection is served by a single Netty event loop. Auth and the catalog handlers do
 * their JDBC on that thread, so a second stream on the same connection cannot start until the first
 * returns. This test holds the first stream in a decorator, the way {@code AuthDecorator} holds it
 * in {@code userRepository.getUserByEmail}, and requires the second stream to enter while that hold
 * is still taken.
 */
class EventLoopStallTest {

  @Test
  void secondStreamStartsWhileFirstIsBlocked() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch fastEntered = new CountDownLatch(1);

    DecoratingHttpServiceFunction blockingAuth =
        (delegate, ctx, req) -> {
          if (req.path().endsWith("/hold")) {
            entered.countDown();
            release.await();
          }
          return delegate.serve(ctx, req);
        };
    DecoratingHttpServiceFunction passThrough = (delegate, ctx, req) -> delegate.serve(ctx, req);

    // Two threads: the second stream needs one while the first is still held.
    BlockingTaskExecutor blockingTaskExecutor =
        BlockingTaskExecutor.builder().numThreads(2).build();
    ArmeriaServerBuilder builder =
        new ArmeriaServerBuilder(
            0, "/api/", "/control/", new ServerProperties(new Properties()), blockingTaskExecutor);
    builder.withSecurityDecorators(passThrough, blockingAuth);
    builder.annotate("probe", new Probe(fastEntered));

    Server server = builder.build();
    assertSecondStreamOverlaps(
        server, blockingTaskExecutor, entered, release, fastEntered, "the blocking decorator");
  }

  @Test
  void secondStreamStartsWhileHandlerIsBlocked() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch fastEntered = new CountDownLatch(1);

    // No security decorators: this is a deployment with authorization disabled, so only
    // useBlockingTaskExecutor can move the handler off the event loop.
    BlockingTaskExecutor blockingTaskExecutor =
        BlockingTaskExecutor.builder().numThreads(2).build();
    ArmeriaServerBuilder builder =
        new ArmeriaServerBuilder(
            0, "/api/", "/control/", new ServerProperties(new Properties()), blockingTaskExecutor);
    builder.annotate("probe", new BlockingProbe(entered, release, fastEntered));

    Server server = builder.build();
    assertSecondStreamOverlaps(
        server, blockingTaskExecutor, entered, release, fastEntered, "the blocking handler");
  }

  @Test
  void stopWaitsForBlockedRequestAndExecutorRemainsActive() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch fastEntered = new CountDownLatch(1);
    BlockingTaskExecutor blockingTaskExecutor =
        BlockingTaskExecutor.builder().numThreads(2).build();
    ArmeriaServerBuilder builder =
        new ArmeriaServerBuilder(
            0, "/api/", "/control/", new ServerProperties(new Properties()), blockingTaskExecutor);
    builder.annotate("probe", new BlockingProbe(entered, release, fastEntered));

    Server server = builder.build();
    try {
      server.start().join();
      int port = server.activeLocalPort();
      WebClient client =
          WebClient.builder("h2c://127.0.0.1:" + port).responseTimeoutMillis(5_000).build();

      CompletableFuture<AggregatedHttpResponse> held = client.get("/api/probe/hold").aggregate();
      assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

      CompletableFuture<Void> stopping = server.stop();
      assertThatThrownBy(() -> stopping.get(250, TimeUnit.MILLISECONDS))
          .isInstanceOf(TimeoutException.class);

      release.countDown();
      assertThat(held.join().status()).isEqualTo(HttpStatus.OK);
      stopping.join();

      assertThat(blockingTaskExecutor.submit(() -> "still active").get()).isEqualTo("still active");
    } finally {
      release.countDown();
      try {
        server.stop().join();
      } finally {
        blockingTaskExecutor.shutdown();
      }
    }
  }

  private static void assertSecondStreamOverlaps(
      Server server,
      BlockingTaskExecutor blockingTaskExecutor,
      CountDownLatch entered,
      CountDownLatch release,
      CountDownLatch fastEntered,
      String blockedOn)
      throws Exception {
    server.start().join();
    try {
      int port = server.activeLocalPort();
      // h2c prior knowledge, one connection: both calls are streams on the same channel, which is
      // what Storium's single JDK HttpClient does against this server.
      WebClient client =
          WebClient.builder("h2c://127.0.0.1:" + port).responseTimeoutMillis(5_000).build();

      CompletableFuture<AggregatedHttpResponse> held = client.get("/api/probe/hold").aggregate();
      assertThat(entered.await(5, TimeUnit.SECONDS))
          .as("the first stream reaches %s", blockedOn)
          .isTrue();

      CompletableFuture<AggregatedHttpResponse> fast = client.get("/api/probe/fast").aggregate();
      boolean overlapped = fastEntered.await(2, TimeUnit.SECONDS);
      release.countDown();

      assertThat(overlapped)
          .as("second stream entered while the first was still blocked in %s", blockedOn)
          .isTrue();
      assertThat(held.join().status()).isEqualTo(HttpStatus.OK);
      assertThat(fast.join().status()).isEqualTo(HttpStatus.OK);
    } finally {
      release.countDown();
      try {
        server.stop().join();
      } finally {
        blockingTaskExecutor.shutdown();
      }
    }
  }

  /** Two routes and no database. The hold is in the decorator, matching production auth. */
  private static final class Probe implements UnityCatalogRestService {

    private final CountDownLatch fastEntered;

    private Probe(CountDownLatch fastEntered) {
      this.fastEntered = fastEntered;
    }

    @Get("/hold")
    public HttpResponse hold() {
      return HttpResponse.of("hold");
    }

    @Get("/fast")
    public HttpResponse fast() {
      fastEntered.countDown();
      return HttpResponse.of("fast");
    }
  }

  /** Hold is inside the handler, so the test fails if handlers still run on the event loop. */
  private static final class BlockingProbe implements UnityCatalogRestService {

    private final CountDownLatch entered;
    private final CountDownLatch release;
    private final CountDownLatch fastEntered;

    private BlockingProbe(
        CountDownLatch entered, CountDownLatch release, CountDownLatch fastEntered) {
      this.entered = entered;
      this.release = release;
      this.fastEntered = fastEntered;
    }

    @Get("/hold")
    public HttpResponse hold() throws InterruptedException {
      entered.countDown();
      release.await();
      return HttpResponse.of("hold");
    }

    @Get("/fast")
    public HttpResponse fast() {
      fastEntered.countDown();
      return HttpResponse.of("fast");
    }
  }
}
