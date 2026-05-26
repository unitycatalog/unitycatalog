package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.ApiClientUtils;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;

/**
 * Creates internal Hadoop credential wrappers from Unity Catalog temporary credential APIs.
 *
 * <p>This is an adapter over SDK-generated API clients, not a generated API client itself.
 *
 * <p><b>Internal API — not for external use. May change without notice.</b>
 */
public interface GenericCredentialFetcher {
  GenericCredential createCredential() throws ApiException;

  /** Creates a fetcher backed by the standard UC temporary credentials API. */
  static GenericCredentialFetcher forUc(
      Configuration conf, io.unitycatalog.client.api.TemporaryCredentialsApi api) {
    return new UCGenericCredentialFetcher(conf, api);
  }

  /** Creates a fetcher backed by the UC Delta temporary credentials API. */
  static GenericCredentialFetcher forUcDelta(
      Configuration conf, io.unitycatalog.client.delta.api.TemporaryCredentialsApi api) {
    return new UCDeltaGenericCredentialFetcher(conf, api);
  }

  /** Creates a fetcher backed by the UC Delta staging table credentials API. */
  static GenericCredentialFetcher forUcDeltaStagingTable(
      Configuration conf, io.unitycatalog.client.delta.api.TemporaryCredentialsApi api) {
    return new UCDeltaStagingTableCredentialFetcher(conf, api);
  }

  /**
   * Creates a {@link GenericCredentialFetcher} from an already-built {@link ApiClient} and a Hadoop
   * configuration containing only the credential-request keys (type, table/path id, operation, and
   * for Delta: catalog/schema/table/location). Auth keys are not required — the provided {@code
   * apiClient} already carries authentication.
   */
  static GenericCredentialFetcher create(ApiClient apiClient, Configuration conf) {
    boolean useDeltaCredentialsApi =
        conf.getBoolean(
            UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY,
            UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_DEFAULT_VALUE);
    if (useDeltaCredentialsApi) {
      String stagingTableId = conf.get(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY);
      if (stagingTableId != null && !stagingTableId.isEmpty()) {
        // Get the credential for the staging table, via UC Delta API.
        return forUcDeltaStagingTable(
            conf, new io.unitycatalog.client.delta.api.TemporaryCredentialsApi(apiClient));
      } else {
        // Get the credentials for the normal table, via UC Delta API.
        return forUcDelta(
            conf, new io.unitycatalog.client.delta.api.TemporaryCredentialsApi(apiClient));
      }
    } else {
      // Get the credentials for either path or table, via legacy UC API.
      return forUc(conf, new io.unitycatalog.client.api.TemporaryCredentialsApi(apiClient));
    }
  }

  static GenericCredentialFetcher create(Configuration conf) {
    String ucUriStr = conf.get(UCHadoopConfConstants.UC_URI_KEY);
    Preconditions.checkNotNull(
        ucUriStr,
        "Failed to create GenericCredentialFetcher, the '%s' is not set in hadoop configuration",
        UCHadoopConfConstants.UC_URI_KEY);
    ApiClient apiClient =
        ApiClientUtils.create(
            URI.create(ucUriStr),
            TokenProvider.create(conf.getPropsWithPrefix(UCHadoopConfConstants.UC_AUTH_PREFIX)),
            UCHadoopConfConstants.createRequestRetryPolicy(conf),
            conf.getPropsWithPrefix(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX));
    return create(apiClient, conf);
  }
}
