package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.utils.Clock;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

public class ApiClientFactory {
  private ApiClientFactory() {}
  /**
   * Constructs a catalog API client using retry settings embedded in a Hadoop
   * {@link Configuration}. This overload is intended for executor-side code
   * (for example, credential providers) that already receives the driver’s
   * serialized `fs.unitycatalog.request.retry.*` keys; defaults are used if
   * the keys are absent.
   */
  public static ApiClient createApiClient(Configuration conf, URI url, String token) {
    ApiClientConf clientConf = conf != null
        ? UCHadoopConf.getApiClientConf(conf)
        : new ApiClientConf();
    return createApiClient(clientConf, url, token);
  }
  
  /**
   * Constructs a catalog API client from an application-supplied
   * {@link ApiClientConf}. Use this overload on the driver or in tests when
   * the caller can define retry behaviour directly without going through
   * Hadoop configuration serialization.
   */
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
