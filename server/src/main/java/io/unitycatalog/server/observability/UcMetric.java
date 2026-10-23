package io.unitycatalog.server.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.Getter;

/**
 * Every UC domain metric, with its name and description. A metric's name is its namespace from
 * {@link UcMetricConstants} followed by the event it counts.
 */
@Getter
public enum UcMetric {
  TABLE_CREATIONS(
      UcMetricConstants.UC_SECURABLE_TABLE,
      UcMetricConstants.CREATIONS,
      "Number of table securables created after their database transactions commit, including"
          + " tables, views, and metric views"),
  STORAGE_CREDENTIAL_CACHE_REQUESTS(
      UcMetricConstants.UC_STORAGE_CREDENTIAL_CACHE,
      UcMetricConstants.REQUESTS,
      "Storage credential requests served from the cache (hit) or that the cache could not serve"
          + " (miss)"),
  STORAGE_CLEANUP_ATTEMPTS(
      UcMetricConstants.UC_STORAGE_CLEANUP,
      UcMetricConstants.ATTEMPTS,
      "Storage cleanup attempts for dropped managed resources that deleted the resource's files"
          + " (success) or failed to delete them or to save the result, and will be retried"
          + " (failure)");

  private final String metricName;
  private final String description;

  UcMetric(String namespace, String event, String description) {
    this.metricName = namespace + UcMetricConstants.DELIMITER + event;
    this.description = description;
  }

  /** Registers this metric as a counter with {@code tags} in {@code registry}. */
  Counter counter(MeterRegistry registry, Tag... tags) {
    return Counter.builder(metricName)
        .description(description)
        .tags(Tags.of(tags))
        .register(registry);
  }
}
