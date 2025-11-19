package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.token.UCTokenProvider;
import io.unitycatalog.spark.utils.Clock;
import java.net.URI;

public class ApiClientFactory {

  public static final String BASE_PATH = "/api/2.1/unity-catalog";

  private ApiClientFactory() {
  }

  public static ApiClient createApiClient(
      ApiClientConf clientConf, URI url, UCTokenProvider ucTokenProvider) {
    // Base path in ApiClient is already set to `BASE_PATH`, so we override it to provide
    // base path from given `url` but still preserving path suffix.
    // Expected input for `url` is URL with no "/api/2.1/unity-catalog" in the path.
    String basePath = url.getPath() + BASE_PATH;
    RetryingApiClient apiClient = new RetryingApiClient(clientConf, Clock.systemClock());
    apiClient.setHost(url.getHost())
        .setPort(url.getPort())
        .setScheme(url.getScheme())
        .setBasePath(basePath);

    if (ucTokenProvider != null) {
      apiClient.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + ucTokenProvider.accessToken())
      );
    }
    return apiClient;
  }
}
