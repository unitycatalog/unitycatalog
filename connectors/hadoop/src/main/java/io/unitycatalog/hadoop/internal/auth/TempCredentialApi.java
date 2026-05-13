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
 */
interface TempCredentialApi {
  GenericCredential createCredential() throws ApiException;

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
