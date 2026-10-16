package io.unitycatalog.server.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

/** Metrics for the storage credential cache ({@code server.storage-credential-cache.*}). */
public final class StorageCredentialCacheMetrics {
  private final Counter hits;
  private final Counter misses;

  StorageCredentialCacheMetrics(MeterRegistry registry) {
    // Register both results up front so that hit-ratio queries have both series from startup.
    hits =
        UcMetric.STORAGE_CREDENTIAL_CACHE_REQUESTS.counter(
            registry, Tag.of(UcMetricConstants.RESULT, UcMetricConstants.HIT));
    misses =
        UcMetric.STORAGE_CREDENTIAL_CACHE_REQUESTS.counter(
            registry, Tag.of(UcMetricConstants.RESULT, UcMetricConstants.MISS));
  }

  /** Records a request served from the cache. */
  public void recordHit() {
    hits.increment();
  }

  /** Records a request the cache could not serve, whether or not the new vend succeeds. */
  public void recordMiss() {
    misses.increment();
  }
}
