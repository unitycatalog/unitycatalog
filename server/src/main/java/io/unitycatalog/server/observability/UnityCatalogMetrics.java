package io.unitycatalog.server.observability;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;

/** Registers UC domain metrics against a server-owned registry. */
public final class UnityCatalogMetrics {
  private final TableMetrics tables;

  /** Registers the domain meters; the caller retains ownership of the registry's lifecycle. */
  public UnityCatalogMetrics(MeterRegistry registry) {
    tables = new TableMetrics(Objects.requireNonNull(registry, "registry"));
  }

  /** Returns the recorder for table lifecycle events. */
  public TableMetrics tables() {
    return tables;
  }
}
