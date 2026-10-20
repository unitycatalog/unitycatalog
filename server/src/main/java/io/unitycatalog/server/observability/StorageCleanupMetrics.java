package io.unitycatalog.server.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.unitycatalog.server.persist.ManagedResourceType;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

/**
 * Counts the storage cleanup worker's attempts by result and resource type ({@link
 * UcMetric#STORAGE_CLEANUP_ATTEMPTS}).
 */
public final class StorageCleanupMetrics {
  private final Map<ManagedResourceType, Counter> successes =
      new EnumMap<>(ManagedResourceType.class);
  private final Map<ManagedResourceType, Counter> failures =
      new EnumMap<>(ManagedResourceType.class);

  StorageCleanupMetrics(MeterRegistry registry) {
    // Register every result and resource type up front so that failure alerts and ratios have all
    // series from startup.
    for (ManagedResourceType type : ManagedResourceType.values()) {
      Tag resourceType =
          Tag.of(UcMetricConstants.RESOURCE_TYPE, type.name().toLowerCase(Locale.ROOT));
      successes.put(type, attempts(registry, UcMetricConstants.SUCCESS, resourceType));
      failures.put(type, attempts(registry, UcMetricConstants.FAILURE, resourceType));
    }
  }

  private static Counter attempts(MeterRegistry registry, String result, Tag resourceType) {
    return UcMetric.STORAGE_CLEANUP_ATTEMPTS.counter(
        registry, Tag.of(UcMetricConstants.RESULT, result), resourceType);
  }

  /** Records an attempt that deleted the resource's files. */
  public void recordSuccess(ManagedResourceType resourceType) {
    successes.get(resourceType).increment();
  }

  /** Records an attempt that failed and leaves its task to be retried. */
  public void recordFailure(ManagedResourceType resourceType) {
    failures.get(resourceType).increment();
  }
}
