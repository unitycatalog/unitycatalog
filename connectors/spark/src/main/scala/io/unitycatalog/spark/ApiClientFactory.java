package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;

import java.net.URI;

public class ApiClientFactory {

  public static final String BASE_PATH = "/api/2.1/unity-catalog";

  private ApiClientFactory() {}

  public static ApiClient createApiClient(URI url, String token) {
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
