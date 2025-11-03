package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.utils.Clock;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.net.http.HttpClient;

public class RetryingApiClient extends ApiClient {

  private HttpRetryHandler retryHandler;

  public RetryingApiClient(Configuration conf) {
    this(conf, Clock.systemClock(), null);
  }

  public RetryingApiClient(Configuration conf, Clock clock, HttpRetryHandler retryHandler) {
    if (retryHandler == null) {
      // Use default handler with configuration (validation happens in constructor)
      this.retryHandler = new DefaultHttpRetryHandler(conf, clock);
    } else {
      this.retryHandler = retryHandler;
    }
  }

  @Override
  public HttpClient getHttpClient() {
    HttpClient baseClient = super.getHttpClient();
    return new RetryingHttpClient(baseClient, retryHandler);
  }

  public static RetryingApiClient create(Configuration conf, URI uri, String token) {
    return ApiClientFactory.configureClient(new RetryingApiClient(conf), uri, token);
  }
}

