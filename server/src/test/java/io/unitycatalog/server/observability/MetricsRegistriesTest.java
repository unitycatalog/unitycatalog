package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

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
  public void closeReleasesRegistryAndIsIdempotent() {
    MetricsRegistries.PrometheusMetrics metrics = MetricsRegistries.createPrometheus();
    metrics.close();
    // JvmGcMetrics.close() is not itself re-entrant, so the holder must guard against a double
    // close (which the server's failure and shutdown paths can both trigger).
    metrics.close();
    assertThat(metrics.registry().isClosed()).isTrue();
  }
}
