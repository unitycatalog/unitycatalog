package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the health endpoints against the exact Armeria wiring {@code UnityCatalogServer} uses:
 * {@code /livez} = {@code HealthCheckService.of()} (no checkers), {@code /readyz} = {@code
 * HealthCheckService} backed by a {@link DbReadinessChecker}. Confirms the /readyz 503/200 mapping
 * and, critically, that /livez is independent of database/readiness state.
 */
public class HealthCheckEndpointsIntegrationTest {

  private final AtomicBoolean dbReachable = new AtomicBoolean(false);
  private DbReadinessChecker checker;
  private Server server;
  private WebClient client;

  @BeforeEach
  public void setUp() {
    checker = new DbReadinessChecker(dbReachable::get, Duration.ofSeconds(5));
    server =
        Server.builder()
            .service("/livez", HealthCheckService.of())
            .service(
                "/readyz", HealthCheckService.builder().checkers(checker.healthChecker()).build())
            .build();
    server.start().join();
    client = WebClient.of("http://127.0.0.1:" + server.activeLocalPort());
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
  public void readinessCheckerDrivesReadyzStatus() {
    // Fail closed: not ready before the first successful probe.
    assertHealth("/readyz", 503, false);

    dbReachable.set(true);
    checker.refresh();
    assertHealth("/readyz", 200, true);

    // Flips back to 503 when the DB becomes unreachable again.
    dbReachable.set(false);
    checker.refresh();
    assertHealth("/readyz", 503, false);
  }

  @Test
  public void livezStaysHealthyWhileReadyzIsNotReady() {
    // DB never reachable (checker stays fail-closed): /readyz is 503, but /livez must remain 200 —
    // liveness never consults the database.
    assertHealth("/readyz", 503, false);
    assertHealth("/livez", 200, true);
  }

  private void assertHealth(String path, int expectedStatus, boolean expectedHealthy) {
    AggregatedHttpResponse response = client.get(path).aggregate().join();
    assertThat(response.status().code()).isEqualTo(expectedStatus);
    assertThat(response.contentUtf8()).contains("\"healthy\":" + expectedHealthy);
  }
}
