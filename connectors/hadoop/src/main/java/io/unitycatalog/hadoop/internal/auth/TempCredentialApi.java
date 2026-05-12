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
public interface TempCredentialApi {
  GenericCredential createCredential() throws ApiException;

  /**
   * Creates a {@link TempCredentialApi} from an already-built {@link ApiClient} and a Hadoop
   * configuration containing only the credential-request keys (type, table/path id, operation, and
   * for Delta: catalog/schema/table/location). Auth keys are not required — the provided {@code
   * apiClient} already carries authentication.
   */
  static TempCredentialApi create(ApiClient apiClient, Configuration conf) {
    boolean useDeltaCredentialsApi =
        conf.getBoolean(
            UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY,
            UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_DEFAULT_VALUE);
    if (useDeltaCredentialsApi) {
      return new UCDeltaTempCredentialApi(
          conf, new io.unitycatalog.client.delta.api.TemporaryCredentialsApi(apiClient));
    }
    return new UCTempCredentialApi(
        conf, new io.unitycatalog.client.api.TemporaryCredentialsApi(apiClient));
  }

  static TempCredentialApi create(Configuration conf) {
    String useDeltaCredentialsApiValue =
        conf.get(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY);
    if (useDeltaCredentialsApiValue != null) {
      useDeltaCredentialsApiValue = useDeltaCredentialsApiValue.trim();
    }
    Preconditions.checkArgument(
        useDeltaCredentialsApiValue == null
            || "true".equalsIgnoreCase(useDeltaCredentialsApiValue)
            || "false".equalsIgnoreCase(useDeltaCredentialsApiValue),
        "Unsupported value '%s' for '%s', expected true or false",
        useDeltaCredentialsApiValue,
        UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY);
    boolean useDeltaCredentialsApi =
        useDeltaCredentialsApiValue == null
            ? UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_DEFAULT_VALUE
            : Boolean.parseBoolean(useDeltaCredentialsApiValue);

    String ucUriStr = conf.get(UCHadoopConfConstants.UC_URI_KEY);
    Preconditions.checkNotNull(
        ucUriStr, "'%s' is not set in hadoop configuration", UCHadoopConfConstants.UC_URI_KEY);

    ApiClient apiClient =
        ApiClientUtils.create(
            URI.create(ucUriStr),
            TokenProvider.create(conf.getPropsWithPrefix(UCHadoopConfConstants.UC_AUTH_PREFIX)),
            UCHadoopConfConstants.createRequestRetryPolicy(conf),
            conf.getPropsWithPrefix(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX));
    if (useDeltaCredentialsApi) {
      return new UCDeltaTempCredentialApi(
          conf, new io.unitycatalog.client.delta.api.TemporaryCredentialsApi(apiClient));
    }
    return new UCTempCredentialApi(
        conf, new io.unitycatalog.client.api.TemporaryCredentialsApi(apiClient));
  }
}
