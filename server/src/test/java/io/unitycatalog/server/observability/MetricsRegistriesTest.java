package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;

public class MetricsRegistriesTest {

  @Test
  public void createPrometheusBindsJvmMetrics() {
    PrometheusMeterRegistry registry = MetricsRegistries.createPrometheus();
    String scrape = registry.scrape();
    assertThat(scrape).contains("jvm_memory_used_bytes");
    assertThat(scrape).contains("jvm_threads_live_threads");
  }
}
