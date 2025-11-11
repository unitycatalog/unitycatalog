package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.utils.Clock;

import java.net.URI;

public class ApiClientFactory {
  private ApiClientFactory() {}

  public static ApiClient createApiClient(ApiClientConf clientConf, URI url, String token) {
    RetryingApiClient apiClient = new RetryingApiClient(clientConf, Clock.systemClock());
    apiClient.setHost(url.getHost())
        .setPort(url.getPort())
        .setScheme(url.getScheme());

    if (token != null && !token.isEmpty()) {
      apiClient.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + token)
      );
    }
    return apiClient;
  }
}
