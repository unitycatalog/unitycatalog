package io.unitycatalog.client.internal;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.retry.RetryPolicy;
import java.net.URI;
import java.util.Map;

public final class ApiClientUtils {

  private ApiClientUtils() {}

  public static ApiClient create(
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
