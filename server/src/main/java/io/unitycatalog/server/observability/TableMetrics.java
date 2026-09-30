package io.unitycatalog.server.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** Metrics for persisted tables, including views and metric views. */
public final class TableMetrics {
  private final Counter tablesPersistedCounter;

  TableMetrics(MeterRegistry registry) {
    tablesPersistedCounter =
        Counter.builder("uc.tables.persisted")
            .description(
                "Number of tables successfully persisted after create transactions commit,"
                    + " including views and metric views")
            .register(registry);
  }

  /** Records a table after its database transaction has committed successfully. */
  public void recordTablePersisted() {
    tablesPersistedCounter.increment();
  }
}
