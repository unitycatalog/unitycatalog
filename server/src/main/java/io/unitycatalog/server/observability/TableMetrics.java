package io.unitycatalog.server.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** Metrics for persisted tables, including views and metric views. */
public final class TableMetrics {
  // TODO: Turn this into a generic metric set class for any UC securable.
  private final Counter tableCreationsCounter;

  TableMetrics(MeterRegistry registry) {
    tableCreationsCounter = UcMetric.TABLE_CREATIONS.counter(registry);
  }

  /** Records a table after its database transaction has committed successfully. */
  public void recordTableCreated() {
    tableCreationsCounter.increment();
  }
}
