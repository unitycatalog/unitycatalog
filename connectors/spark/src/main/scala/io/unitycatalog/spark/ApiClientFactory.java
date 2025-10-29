package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

public class ApiClientFactory {
  private ApiClientFactory() {}

  public static ApiClient createApiClient(URI url, String token) {
    ApiClient apiClient = new ApiClient()
        .setHost(url.getHost())
        .setPort(url.getPort())
        .setScheme(url.getScheme());

    if (token != null && !token.isEmpty()) {
      apiClient = apiClient.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + token)
      );
    }

    return apiClient;
  }

  public static ApiClient createApiClient(Configuration conf, URI url, String token) {
    boolean retryEnabled = conf.getBoolean(
        UCHadoopConf.RETRY_ENABLED_KEY,
        UCHadoopConf.RETRY_ENABLED_DEFAULT);

    if (retryEnabled) {
      return RetryingApiClient.create(conf, url, token);
    } else {
      return createApiClient(url, token);
    }
  }
}
