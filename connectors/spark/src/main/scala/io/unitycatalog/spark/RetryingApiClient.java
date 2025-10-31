package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.utils.Clock;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.net.http.HttpClient;

public class RetryingApiClient extends ApiClient {

  private final Configuration conf;
  private final Clock clock;
  private HttpRetryHandler retryHandler;

  public RetryingApiClient(Configuration conf) {
    this(conf, Clock.systemClock(), null);
  }

  public RetryingApiClient(Configuration conf, Clock clock) {
    this(conf, clock, null);
  }

  public RetryingApiClient(Configuration conf, Clock clock, HttpRetryHandler retryHandler) {
    super();

    this.conf = conf;
    this.clock = clock;

    if (retryHandler == null) {
      // Use default handler with configuration (validation happens in constructor)
      this.retryHandler = new DefaultHttpRetryHandler(conf, clock);
    } else {
      this.retryHandler = retryHandler;
    }
  }

  /**
   * Sets a custom retry handler for this API client.
   *
   * @param handler The retry handler to use
   * @return This object for method chaining
   */
  public RetryingApiClient setRetryHandler(HttpRetryHandler handler) {
    this.retryHandler = handler;
    return this;
  }

  @Override
  public HttpClient getHttpClient() {
    HttpClient baseClient = super.getHttpClient();
    return new RetryingHttpClient(baseClient, retryHandler);
  }

  public static RetryingApiClient create(Configuration conf, URI uri, String token) {
    RetryingApiClient client = new RetryingApiClient(conf);
    client.setHost(uri.getHost())
        .setScheme(uri.getScheme())
        .setPort(uri.getPort())
        .setBasePath(uri.getPath());

    if (token != null && !token.isEmpty()) {
      client.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + token));
    }

    return client;
  }
}

