package io.unitycatalog.server.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** Metrics for persisted tables, including views and metric views. */
public final class TableMetrics {
  private final Counter tableCreatedCounter;

  TableMetrics(MeterRegistry registry) {
    tableCreatedCounter =
        Counter.builder("uc.securable.table.created")
            .description(
                "Number of table securables created after their database transactions commit,"
                    + " including tables, views, and metric views")
            .register(registry);
  }

  /** Records a table after its database transaction has committed successfully. */
  public void recordTableCreated() {
    tableCreatedCounter.increment();
  }
}
