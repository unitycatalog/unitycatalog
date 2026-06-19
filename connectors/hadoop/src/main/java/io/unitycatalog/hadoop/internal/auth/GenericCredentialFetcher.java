package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.ApiClientUtils;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.id.CredId;
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import io.unitycatalog.hadoop.internal.id.DeltaTableCredId;
import io.unitycatalog.hadoop.internal.id.PathCredId;
import io.unitycatalog.hadoop.internal.id.TableCredId;
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

  /** Creates a fetcher backed by the standard UC temporary credentials API for a table. */
  static GenericCredentialFetcher forUc(
      TableCredId credId, io.unitycatalog.client.api.TemporaryCredentialsApi api) {
    return new UCGenericCredentialFetcher(credId, api);
  }

  /** Creates a fetcher backed by the standard UC temporary credentials API for a path. */
  static GenericCredentialFetcher forUc(
      PathCredId credId, io.unitycatalog.client.api.TemporaryCredentialsApi api) {
    return new UCGenericCredentialFetcher(credId, api);
  }

  /** Creates a fetcher backed by the UC Delta temporary credentials API. */
  static GenericCredentialFetcher forUcDelta(
      DeltaTableCredId credId, io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi api) {
    return new UCDeltaGenericCredentialFetcher(credId, api);
  }

  /** Creates a fetcher backed by the UC Delta staging table credentials API. */
  static GenericCredentialFetcher forUcDeltaStagingTable(
      DeltaStagingTableCredId credId,
      io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi api) {
    return new UCDeltaStagingTableCredentialFetcher(credId, api);
  }

  /**
   * Creates a {@link GenericCredentialFetcher} from an already-built {@link ApiClient} and the
   * {@link CredId} identifying the credential scope. The fetcher subtype is selected from the
   * {@code credId} type. Auth keys are not required — the provided {@code apiClient} already
   * carries authentication.
   */
  static GenericCredentialFetcher create(ApiClient apiClient, CredId credId) {
    if (credId instanceof DeltaStagingTableCredId) {
      return forUcDeltaStagingTable(
          (DeltaStagingTableCredId) credId,
          new io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi(apiClient));
    } else if (credId instanceof DeltaTableCredId) {
      return forUcDelta(
          (DeltaTableCredId) credId,
          new io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi(apiClient));
    } else if (credId instanceof PathCredId) {
      return forUc(
          (PathCredId) credId, new io.unitycatalog.client.api.TemporaryCredentialsApi(apiClient));
    } else if (credId instanceof TableCredId) {
      return forUc(
          (TableCredId) credId, new io.unitycatalog.client.api.TemporaryCredentialsApi(apiClient));
    } else {
      throw new IllegalArgumentException("Unsupported CredId type: " + credId);
    }
  }

  static GenericCredentialFetcher create(Configuration conf) {
    String ucUriStr = conf.get(UCHadoopConfConstants.UC_URI_KEY);
    Preconditions.checkNotNull(
        ucUriStr,
        "Failed to create GenericCredentialFetcher, the '%s' is not set in hadoop configuration",
        UCHadoopConfConstants.UC_URI_KEY);
    URI ucUri = URI.create(ucUriStr);
    ApiClient apiClient =
        ApiClientUtils.create(
            ucUri,
            TokenProvider.create(conf.getPropsWithPrefix(UCHadoopConfConstants.UC_AUTH_PREFIX)),
            UCHadoopConfConstants.createRequestRetryPolicy(conf),
            conf.getPropsWithPrefix(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX));
    return create(apiClient, CredId.create(conf));
  }
}
