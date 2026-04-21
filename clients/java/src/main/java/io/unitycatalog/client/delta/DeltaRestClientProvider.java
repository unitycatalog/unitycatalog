package io.unitycatalog.client.delta;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.delta.api.TablesApi;
import java.util.Optional;

/**
 * Interface for catalog implementations that can provide a Delta REST Catalog {@link TablesApi}
 * client. This enables a single shared client instance owned by the catalog (UCProxy) to be reused
 * by all consumers -- AbstractDeltaCatalog, UCCommitCoordinatorClient, and the Kernel v2 connector
 * -- instead of each creating their own HTTP client to the same UC server.
 *
 * <p>Implementations should return the same cached {@link TablesApi} instance on every call. The
 * caller must not close or modify the returned client.
 *
 * <p>This interface lives in {@code unitycatalog-client} so that both the UC Spark connector (which
 * implements it) and Delta Spark (which consumes it) can depend on it without a circular
 * dependency.
 */
public interface DeltaRestClientProvider {

  /**
   * Returns the shared Delta REST Catalog {@link TablesApi} client when the Delta REST Catalog
   * integration is enabled for this catalog (see the {@code deltaRestApi.enabled} option), or
   * {@link Optional#empty()} when it is disabled. The returned instance is owned by the catalog and
   * must not be closed by the caller.
   *
   * <p>The current Delta Spark consumer builds its own {@link TablesApi} from {@link
   * #getApiClient()} rather than reusing this cached instance; this method is intended for future
   * consumers (commit coordinator, Kernel v2 connector) that want to share UC's cached client
   * instead of constructing their own.
   *
   * @return the cached DRC {@link TablesApi} when DRC is enabled, otherwise {@link
   *     Optional#empty()} — indicating the caller should fall back to the legacy UC API via {@link
   *     #getApiClient()}.
   */
  Optional<TablesApi> getDeltaTablesApi();

  /**
   * Returns the base {@link ApiClient} for UC API access. Consumers use it to construct typed
   * wrappers (legacy {@code TablesApi}, {@code DeltaCommitsApi}, DRC {@code TablesApi}, etc.)
   * against UC's shared HTTP pool. This exists so consumers don't build a duplicate {@link
   * ApiClient} from URL + token (as Delta did previously) and end up with two HTTP connection pools
   * pointing at the same UC server. Implementations must return a non-null client.
   */
  ApiClient getApiClient();
}
