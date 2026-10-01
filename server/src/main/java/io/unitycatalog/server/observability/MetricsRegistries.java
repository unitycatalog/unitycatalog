package io.unitycatalog.server.observability;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.function.Predicate;

/** Builds a Prometheus meter registry with standard JVM/system instrumentation. */
public final class MetricsRegistries {

  private MetricsRegistries() {}

  /**
   * A Prometheus registry together with the one binder that holds a JVM resource: the
   * GC-notification listeners {@link JvmGcMetrics} registers on the GC MXBeans. Each server
   * instance owns and closes its registry and binder. (The other bound metrics — memory, threads,
   * processor — register only gauges and need no close.)
   */
  public static final class PrometheusMetrics implements AutoCloseable {
    private final PrometheusMeterRegistry registry;
    private final JvmGcMetrics jvmGcMetrics;
    private boolean closed;

    private PrometheusMetrics(PrometheusMeterRegistry registry, JvmGcMetrics jvmGcMetrics) {
      this.registry = registry;
      this.jvmGcMetrics = jvmGcMetrics;
    }

    public PrometheusMeterRegistry registry() {
      return registry;
    }

    // Idempotent: JvmGcMetrics.close() removes its MXBean listeners and is not safe to call twice.
    // Best-effort: the registry is closed even if closing the GC binder throws.
    @Override
    public synchronized void close() {
      if (closed) {
        return;
      }
      closed = true;
      try {
        jvmGcMetrics.close();
      } finally {
        registry.close();
      }
    }
  }

  public static PrometheusMetrics createPrometheus() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    Predicate<Meter.Id> httpRequestCounter = id -> id.getName().equals("http.server.requests");
    registry
        .config()
        .meterFilter(
            MeterFilter.deny(
                id -> id.getName().startsWith("http.server.") && !httpRequestCounter.test(id)))
        .meterFilter(MeterFilter.forMeters(httpRequestCounter, MeterFilter.ignoreTags("result")));
    new JvmMemoryMetrics().bindTo(registry);
    JvmGcMetrics jvmGcMetrics = new JvmGcMetrics();
    jvmGcMetrics.bindTo(registry);
    new JvmThreadMetrics().bindTo(registry);
    new ProcessorMetrics().bindTo(registry);
    return new PrometheusMetrics(registry, jvmGcMetrics);
  }
}
