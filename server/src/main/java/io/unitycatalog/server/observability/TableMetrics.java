package io.unitycatalog.server.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** Metrics for persisted table securables, including views and metric views. */
public final class TableMetrics {
  private final Counter tablesCreated;

  TableMetrics(MeterRegistry registry) {
    tablesCreated =
        Counter.builder("uc.tables.created")
            .description(
                "Number of table securables successfully persisted through a create-table API,"
                    + " including views and metric views")
            .register(registry);
  }

  /** Records a create after its database transaction has committed successfully. */
  public void recordCreated() {
    tablesCreated.increment();
  }
}
