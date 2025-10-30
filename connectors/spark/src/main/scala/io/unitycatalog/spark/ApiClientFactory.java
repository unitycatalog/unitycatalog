package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.utils.Clock;
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
    return createApiClient(conf, url, token, null);
  }

  public static ApiClient createApiClient(
      Configuration conf,
      URI url,
      String token,
      HttpRetryHandler retryHandler) {
    boolean retryEnabled = conf.getBoolean(
        UCHadoopConf.RETRY_ENABLED_KEY,
        UCHadoopConf.RETRY_ENABLED_DEFAULT);

    // If a custom retry handler is provided, enable retrying even if retry is disabled in config
    if (retryEnabled || retryHandler != null) {
      RetryingApiClient client = retryHandler != null
          ? new RetryingApiClient(conf, Clock.systemClock(), retryHandler)
          : new RetryingApiClient(conf);

      client.setHost(url.getHost())
          .setScheme(url.getScheme())
          .setPort(url.getPort())
          .setBasePath(url.getPath());

      if (token != null && !token.isEmpty()) {
        client.setRequestInterceptor(
            request -> request.header("Authorization", "Bearer " + token));
      }

      return client;
    } else {
      return createApiClient(url, token);
    }
  }
}
