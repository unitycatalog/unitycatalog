package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.base.BaseServerTest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

/**
 * Verifies the observability endpoints are served only on the dedicated observability port and are
 * not exposed on the main API listener. This is the guarantee behind moving {@code /metrics} (and
 * the probes) off the serving interface: an API-ingress misconfiguration cannot leak them because
 * they are 404 on that port. Runs against the real {@link
 * io.unitycatalog.server.UnityCatalogServer} wiring (two ports on one server).
 */
public class ObservabilityPortIsolationTest extends BaseServerTest {

  @Test
  public void observabilityEndpointsAreServedOnlyOnTheObservabilityPort() {
    // Present on the observability port.
    assertThat(httpGetObservability("/livez").statusCode()).isEqualTo(200);
    assertThat(httpGetObservability("/readyz").statusCode()).isEqualTo(200);
    assertThat(httpGetObservability("/metrics").statusCode()).isEqualTo(200);

    // Absent from the API port.
    assertThat(httpGet("/livez").statusCode()).isEqualTo(404);
    assertThat(httpGet("/readyz").statusCode()).isEqualTo(404);
    assertThat(httpGet("/metrics").statusCode()).isEqualTo(404);
  }

  @Test
  public void apiIsServedOnlyOnTheApiPort() {
    // The root banner answers on the API port ...
    HttpResponse<String> apiRoot = httpGet("/");
    assertThat(apiRoot.statusCode()).isEqualTo(200);
    assertThat(apiRoot.body()).contains("Hello, Unity Catalog!");

    // ... and the whole API surface is 404 on the observability port -- the banner, the docs, and a
    // real API route alike. Each listener exposes exactly one surface, even though Armeria's
    // default
    // virtual host is otherwise served on every bound port. Checking a real API route (not just the
    // banner) guards against a future registration leaking onto both ports.
    assertThat(httpGetObservability("/").statusCode()).isEqualTo(404);
    assertThat(httpGetObservability("/docs").statusCode()).isEqualTo(404);
    assertThat(httpGetObservability("/api/2.1/unity-catalog/catalogs").statusCode()).isEqualTo(404);
  }
}
