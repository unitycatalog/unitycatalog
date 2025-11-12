package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;

import java.net.URI;

public class ApiClientFactory {

  public static final String BASE_PATH = "/api/2.1/unity-catalog";

  private ApiClientFactory() {}

  public static ApiClient createApiClient(URI url, String token) {
    // Base path in ApiClient is already set to `BASE_PATH`, so we override it to provide
    // base path from given `url` but still preserving path suffix.
    // Expected input for `url` is URL with no "/api/2.1/unity-catalog" in the path.
    String basePath = url.getPath() + BASE_PATH;
    ApiClient apiClient = new ApiClient()
        .setHost(url.getHost())
        .setPort(url.getPort())
        .setScheme(url.getScheme())
        .setBasePath(basePath);

    if (token != null && !token.isEmpty()) {
      apiClient = apiClient.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + token)
      );
    }

    return apiClient;
  }
}
