package io.unitycatalog.client.delta;

import io.unitycatalog.client.delta.api.TablesApi;

/**
 * Interface for catalog implementations that can provide a Delta REST Catalog
 * {@link TablesApi} client. This enables a single shared client instance owned
 * by the catalog (UCProxy) to be reused by all consumers -- AbstractDeltaCatalog,
 * UCCommitCoordinatorClient, and the Kernel v2 connector -- instead of each
 * creating their own HTTP client to the same UC server.
 *
 * <p>Implementations should return the same cached {@link TablesApi} instance
 * on every call. The caller must not close or modify the returned client.
 *
 * <p>This interface lives in {@code unitycatalog-client} so that both the UC Spark
 * connector (which implements it) and Delta Spark (which consumes it) can depend on it
 * without a circular dependency.
 */
public interface DeltaRestClientProvider {

  /**
   * Returns the shared Delta REST Catalog {@link TablesApi} client, or {@code null}
   * when the Delta REST Catalog integration is disabled for this catalog (see
   * the {@code deltaRestApi.enabled} option). Callers must null-check.
   * The returned instance is owned by the catalog and must not be closed by the caller.
   */
  TablesApi getDeltaTablesApi();

  /**
   * Returns the base {@link io.unitycatalog.client.ApiClient} for legacy UC API access,
   * or {@code null} when the implementation does not expose its client. Returned as
   * {@link Object} so that callers in modules that do not depend on the generated
   * client package can receive it opaquely and cast when they need it.
   */
  default Object getApiClient() { return null; }
}
