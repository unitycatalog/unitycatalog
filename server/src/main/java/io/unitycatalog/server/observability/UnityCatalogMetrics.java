package io.unitycatalog.server.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.util.Objects;
import lombok.Getter;

/**
 * Registers UC domain metrics against a server-owned registry, one metric set per area. Components
 * that record metrics take this class and read the areas they need.
 *
 * <p>To add a metric, add an entry to {@link UcMetric}, register it in its area's constructor, and
 * list it in {@code docs/server/observability.md}. A tag that takes its values from user data, such
 * as table names, needs a {@code MeterFilter.maximumAllowableTags} cap in {@link
 * MetricsRegistries}.
 */
@Getter
public final class UnityCatalogMetrics {
  private final TableMetrics tables;
  private final StorageCredentialCacheMetrics storageCredentialCache;

  /** Registers the domain meters; the caller retains ownership of the registry's lifecycle. */
  public UnityCatalogMetrics(MeterRegistry registry) {
    Objects.requireNonNull(registry, "registry");
    tables = new TableMetrics(registry);
    storageCredentialCache = new StorageCredentialCacheMetrics(registry);
  }

  /** Metrics that record nothing, for servers and tests that do not expose metrics. */
  public static UnityCatalogMetrics noop() {
    return new UnityCatalogMetrics(new CompositeMeterRegistry());
  }
}
