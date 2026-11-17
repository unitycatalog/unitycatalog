package io.unitycatalog.server.observability;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.Set;
import java.util.function.Predicate;

/** Builds a Prometheus meter registry with standard JVM/system instrumentation. */
public final class MetricsRegistries {

  /** The only Armeria {@code http.server.*} meters exposed; every other one is denied. */
  private static final Set<String> EXPOSED_HTTP_SERVER_METERS =
      Set.of("http.server.requests", "http.server.total.duration", "http.server.active.requests");

  /**
   * Turns off the client-side percentiles Armeria adds to its timers by default, so a timer exposes
   * only {@code _count}, {@code _sum}, and {@code _max}. Such percentiles cannot be aggregated
   * across servers and add one series per percentile.
   */
  private static final MeterFilter NO_PERCENTILES =
      new MeterFilter() {
        @Override
        public DistributionStatisticConfig configure(
            Meter.Id id, DistributionStatisticConfig config) {
          return DistributionStatisticConfig.builder().percentiles().build().merge(config);
        }
      };

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
                id ->
                    id.getName().startsWith("http.server.")
                        && !EXPOSED_HTTP_SERVER_METERS.contains(id.getName())))
        .meterFilter(MeterFilter.forMeters(httpRequestCounter, MeterFilter.ignoreTags("result")))
        .meterFilter(
            MeterFilter.forMeters(
                id -> id.getName().equals("http.server.total.duration"), NO_PERCENTILES));
    new JvmMemoryMetrics().bindTo(registry);
    JvmGcMetrics jvmGcMetrics = new JvmGcMetrics();
    jvmGcMetrics.bindTo(registry);
    new JvmThreadMetrics().bindTo(registry);
    new ProcessorMetrics().bindTo(registry);
    return new PrometheusMetrics(registry, jvmGcMetrics);
  }
}
