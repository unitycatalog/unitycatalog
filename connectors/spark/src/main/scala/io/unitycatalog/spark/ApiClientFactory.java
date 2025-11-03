package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.utils.Clock;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

public class ApiClientFactory {
  private ApiClientFactory() {}

  static <T extends ApiClient> T configureClient(T client, URI url, String token) {
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

  public static ApiClient createApiClient(URI url, String token) {
    return configureClient(new ApiClient(), url, token);
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
      return configureClient(client, url, token);
    } else {
      return createApiClient(url, token);
    }
  }
}
