package io.unitycatalog.client;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.retry.RetryPolicy;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

/** The canonical entry point for constructing an {@link ApiClient}. */
public final class ApiClientFactory {

  private ApiClientFactory() {}

  public static ApiClient createApiClient(
      URI uri, TokenProvider tokenProvider, RetryPolicy retryPolicy) {
    return createApiClient(uri, tokenProvider, retryPolicy, Collections.emptyMap());
  }

  public static ApiClient createApiClient(
      URI uri,
      TokenProvider tokenProvider,
      RetryPolicy retryPolicy,
      Map<String, String> appVersions) {
    Preconditions.checkNotNull(appVersions, "appVersions cannot be null");
    ApiClientBuilder builder =
        ApiClientBuilder.create().uri(uri).tokenProvider(tokenProvider).retryPolicy(retryPolicy);
    appVersions.forEach(builder::addAppVersion);
    return builder.build();
  }
}
