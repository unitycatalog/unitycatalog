package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.utils.Clock;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

public class ApiClientFactory {
  private ApiClientFactory() {}

  public static ApiClient createApiClient(Configuration conf, URI url, String token) {
    return createApiClient(conf, url, token, null);
  }

  public static ApiClient createApiClient(
      Configuration conf,
      URI url,
      String token,
      HttpRetryHandler retryHandler) {
    RetryingApiClient client = retryHandler != null
        ? new RetryingApiClient(conf, Clock.systemClock(), retryHandler)
        : new RetryingApiClient(conf);
    client.setHost(url.getHost())
        .setPort(url.getPort())
        .setScheme(url.getScheme());
    if (token != null && !token.isEmpty()) {
      client.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + token)
      );
    }
    return client;
  }
}
