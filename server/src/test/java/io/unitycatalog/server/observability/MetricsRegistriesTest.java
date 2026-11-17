package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public class MetricsRegistriesTest {

  @Test
  public void createPrometheusBindsJvmMetrics() {
    try (MetricsRegistries.PrometheusMetrics metrics = MetricsRegistries.createPrometheus()) {
      String scrape = metrics.registry().scrape();
      assertThat(scrape).contains("jvm_memory_used_bytes");
      assertThat(scrape).contains("jvm_threads_live_threads");
    }
  }

  @Test
  public void createPrometheusExposesOnlyAllowedHttpServerMeters() {
    try (MetricsRegistries.PrometheusMetrics metrics = MetricsRegistries.createPrometheus()) {
      PrometheusMeterRegistry registry = metrics.registry();
      Gauge.builder("http.server.active.requests", () -> 0).register(registry);
      registry.counter("http.server.requests", "result", "success").increment();
      // Armeria registers its timers with client-side percentiles; the registry must drop them,
      // and must not expose a histogram either.
      Timer.builder("http.server.total.duration")
          .publishPercentiles(0.5, 0.99)
          .publishPercentileHistogram()
          .register(registry)
          .record(Duration.ofMillis(5));
      registry.timer("http.server.request.duration").record(Duration.ofMillis(5));
      registry.summary("http.server.response.length").record(10);
      // A timer outside the http.server family keeps its percentiles.
      Timer.builder("other.timer")
          .publishPercentiles(0.5)
          .register(registry)
          .record(Duration.ofMillis(5));

      assertThat(registry.scrape())
          .contains(
              "http_server_requests_total 1.0",
              "http_server_total_duration_seconds_count 1",
              "http_server_total_duration_seconds_sum",
              "http_server_total_duration_seconds_max",
              "http_server_active_requests 0.0",
              "other_timer_seconds{quantile=\"0.5\"}")
          .doesNotContain(
              "result=",
              "http_server_total_duration_seconds{quantile=",
              "http_server_total_duration_seconds_bucket",
              "http_server_request_duration",
              "http_server_response_length");
    }
  }

  @Test
  public void closeReleasesRegistryAndIsIdempotent() {
    MetricsRegistries.PrometheusMetrics metrics = MetricsRegistries.createPrometheus();
    metrics.close();
    // JvmGcMetrics.close() is not itself re-entrant, so the holder must guard against a double
    // close (which the server's failure and shutdown paths can both trigger).
    metrics.close();
    assertThat(metrics.registry().isClosed()).isTrue();
  }
}
