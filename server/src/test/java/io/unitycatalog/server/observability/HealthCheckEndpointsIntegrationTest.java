package io.unitycatalog.server.observability;

import static io.unitycatalog.server.utils.TestUtils.sendRawGet;
import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import io.unitycatalog.server.base.ServerConfig;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the same two-port health topology {@code UnityCatalogServer} uses: a single {@link
 * Server} with an API port and a dedicated observability port, where {@code /livez} and {@code
 * /readyz} are bound to a port-based virtual host on the observability port. Confirms the /readyz
 * 503/200 mapping, that /livez is independent of readiness, that both ports live on one server, and
 * that the probes are not exposed on the API port.
 */
public class HealthCheckEndpointsIntegrationTest {

  private final AtomicBoolean dbReachable = new AtomicBoolean(false);
  private DbReadinessChecker checker;
  private Server server;
  private ServerConfig observabilityServerConfig;
  private ServerConfig apiServerConfig;

  @BeforeEach
  public void setUp() throws IOException {
    int[] ports = findTwoFreePorts();
    int apiPort = ports[0];
    int observabilityPort = ports[1];
    checker = new DbReadinessChecker(dbReachable::get, Duration.ofSeconds(5));
    server =
        Server.builder()
            .http(apiPort)
            // Second port on the SAME server; the observability endpoints bind to its port-based
            // virtual host below.
            .http(observabilityPort)
            // A drain window so a graceful stop() keeps the ports open long enough to observe the
            // HealthCheckService 503 flip.
            .gracefulShutdownTimeout(Duration.ofMillis(500), Duration.ofSeconds(3))
            // Root banner on the default virtual host stands in for the API surface on the API
            // port.
            .service("/", (ctx, req) -> HttpResponse.of("Hello, Unity Catalog!"))
            .virtualHost(observabilityPort)
            .service("/livez", HealthCheckService.of())
            .service(
                "/readyz", HealthCheckService.builder().checkers(checker.healthChecker()).build())
            .and()
            .build();
    server.start().join();
    observabilityServerConfig = new ServerConfig("http://127.0.0.1:" + observabilityPort, "");
    apiServerConfig = new ServerConfig("http://127.0.0.1:" + apiPort, "");
  }

  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop().join();
    }
    if (checker != null) {
      checker.close();
    }
  }

  @Test
  public void bindsBothPortsOnASingleServer() {
    // One server, two ports (API + observability) -- never a second Server instance -- so both
    // listeners share one lifecycle and start and stop together.
    long distinctPorts =
        server.activePorts().values().stream()
            .map(port -> port.localAddress().getPort())
            .distinct()
            .count();
    assertThat(distinctPorts).isEqualTo(2L);
  }

  @Test
  public void readinessCheckerDrivesReadyzStatus() throws Exception {
    // Fail closed: not ready before the first successful probe.
    assertHealth(observabilityServerConfig, "/readyz", 503, false);

    dbReachable.set(true);
    checker.refresh();
    assertHealth(observabilityServerConfig, "/readyz", 200, true);

    // Flips back to 503 when the DB becomes unreachable again.
    dbReachable.set(false);
    checker.refresh();
    assertHealth(observabilityServerConfig, "/readyz", 503, false);
  }

  @Test
  public void livezStaysHealthyWhileReadyzIsNotReady() throws Exception {
    // DB never reachable (checker stays fail-closed): /readyz is 503, but /livez must remain 200 --
    // liveness never consults the database.
    assertHealth(observabilityServerConfig, "/readyz", 503, false);
    assertHealth(observabilityServerConfig, "/livez", 200, true);
  }

  @Test
  public void probesAreNotExposedOnTheApiPort() throws Exception {
    // The port-based virtual host registers the probes on the observability port only, so they are
    // 404 on the API port -- an API-ingress misconfiguration cannot reach them.
    assertThat(statusCode(apiServerConfig, "/livez")).isEqualTo(404);
    assertThat(statusCode(apiServerConfig, "/readyz")).isEqualTo(404);
  }

  @Test
  public void probesDrainTo503OnGracefulShutdown() throws Exception {
    // Healthy before shutdown, so the flip below is the drain effect and not the fail-closed state.
    dbReachable.set(true);
    checker.refresh();
    assertHealth(observabilityServerConfig, "/readyz", 200, true);
    assertHealth(observabilityServerConfig, "/livez", 200, true);

    // Graceful shutdown flips HealthCheckService to 503 for the drain window. Observing that on the
    // port-based observability vhost is what proves the server-wide shutdown hook still fires there
    // after the move off the default virtual host.
    CompletableFuture<Void> stopping = server.stop();
    try {
      pollUntilStatus(observabilityServerConfig, "/livez", 503, Duration.ofSeconds(2));
      assertThat(statusCode(observabilityServerConfig, "/livez")).isEqualTo(503);
      assertThat(statusCode(observabilityServerConfig, "/readyz")).isEqualTo(503);
    } finally {
      stopping.join();
    }
  }

  private void assertHealth(
      ServerConfig config, String path, int expectedStatus, boolean expectedHealthy)
      throws Exception {
    java.net.http.HttpResponse<String> response = sendRawGet(config, path);
    assertThat(response.statusCode()).isEqualTo(expectedStatus);
    assertThat(response.body()).contains("\"healthy\":" + expectedHealthy);
  }

  private static int statusCode(ServerConfig config, String path) throws Exception {
    return sendRawGet(config, path).statusCode();
  }

  /**
   * Polls {@code path} until it returns {@code expected}. Requests during the drain window keep the
   * port open, and connection errors mid-shutdown are treated as "not yet" rather than failing.
   */
  private static void pollUntilStatus(
      ServerConfig config, String path, int expected, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        if (statusCode(config, path) == expected) {
          return;
        }
      } catch (IOException stillShuttingDown) {
        // The server may be mid-shutdown; keep trying within the window.
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private static int[] findTwoFreePorts() throws IOException {
    try (ServerSocket first = new ServerSocket(0);
        ServerSocket second = new ServerSocket(0)) {
      return new int[] {first.getLocalPort(), second.getLocalPort()};
    }
  }
}
