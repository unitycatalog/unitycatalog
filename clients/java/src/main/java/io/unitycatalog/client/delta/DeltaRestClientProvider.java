package io.unitycatalog.client.delta;

import io.unitycatalog.client.delta.api.TablesApi;

/**
 * Exposes the shared Delta REST Catalog (DRC) client resources owned by a UC
 * connector catalog. Implementers are discovered via pattern match on Spark
 * catalog instances (e.g. {@code UCSingleCatalog}, {@code UCProxy}).
 *
 * <p>When DRC is disabled for the owning catalog, {@link #getDeltaTablesApi()}
 * returns {@code null}. Callers treat a non-null return as the single signal
 * that DRC is enabled.
 */
public interface DeltaRestClientProvider {

  /**
   * Returns the cached DRC {@link TablesApi} bound to the shared
   * {@code ApiClient}, or {@code null} when DRC is disabled for this catalog.
   * Callers must not close the returned instance.
   */
  TablesApi getDeltaTablesApi();

  /**
   * Returns the shared base {@code ApiClient} for callers that need to build
   * their own API wrappers (e.g. Delta's {@code UCDeltaClient}). Default
   * implementation returns {@code null}; implementers override to expose it.
   */
  default Object getApiClient() {
    return null;
  }
}
